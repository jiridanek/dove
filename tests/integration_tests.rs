/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io;
use std::thread;
use std::time;

extern crate dove;

use dove::core::*;
use dove::error::*;
use dove::sasl::*;
use dove::*;

#[test]
fn client() {
    let mut opts = ConnectionOptions::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");
    opts.username = Some("test".to_string());
    opts.password = Some("test".to_string());
    opts.sasl_mechanism = Some(SaslMechanism::Plain);
    let mut connection = connect("localhost", 5672, opts).expect("Error opening connection");

    let mut driver = ConnectionDriver::new();

    let client = driver.register(connection);

    let mut event_buffer = EventBuffer::new();

    loop {
        match driver.poll(&mut event_buffer) {
            Ok(Some(handle)) => {
                let conn = driver.connection(&handle).unwrap();
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit => {
                            println!("Opening connection!");
                            conn.open();
                        }
                        Event::RemoteOpen(_) => {
                            println!("Remote opened!");
                            let session = conn.create_session();
                            session.open();
                        }
                        Event::RemoteBegin(chan, _) => {
                            println!("Remote begin");
                            let session = conn.get_session(chan).unwrap();
                            let sender = session.create_sender(Some("a"));
                            sender.open();
                            /*
                            conn.close(Some(ErrorCondition {
                                condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                                description: "Buhuu".to_string(),
                            }))
                            */
                        }
                        Event::RemoteClose(close) => {
                            println!(
                                "Received close from peer ({:?}), closing connection!",
                                close
                            );
                            conn.close(None);
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Ok(None) => {
                thread::sleep(time::Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}

//#[test]
fn server() {
    let mut listener = listen(
        "localhost",
        5672,
        ListenOptions {
            container_id: "ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4",
        },
    )
    .expect("Error creating listener");

    let mut driver = ConnectionDriver::new();

    let connection = listener.accept().unwrap();

    let handle = driver.register(connection);

    let mut event_buffer = EventBuffer::new();
    loop {
        match driver.poll(&mut event_buffer) {
            Ok(Some(handle)) => {
                let conn = driver.connection(&handle).unwrap();
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit => {}
                        Event::RemoteOpen(_) => {
                            println!("Remote opened!");
                            conn.open();
                        }
                        Event::RemoteBegin(chan, _) => {
                            println!("Remote begin");

                            let session = conn.get_session(chan).unwrap();
                            session.open();
                        }
                        Event::RemoteClose(_) => {
                            println!("Received close from peer, closing connection!");
                            conn.close(None);
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Ok(None) => {
                thread::sleep(time::Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}
