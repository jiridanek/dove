/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use ::mio::Poll;
use ::mio::Token;
use ::mio::Waker;
use dove::conn::*;
use dove::framing::*;
use dove::message::*;
use dove::sasl::*;
use dove::transport::*;
use dove::types::*;
use std::env;
use std::io::ErrorKind;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use futures::executor::block_on;
use dove::driver::ConnectionDriver;
use dove::error::AmqpError::IoError;
use ::mio::Events;

/**
 * Example client that sends a single message to an AMQP endpoint with minimal dependencies and sending
 * and receiving frames directly on a connection.
 */
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        println!("Usage: ./example_send_framing localhost 5672 myqueue 'Hello, world'");
        std::process::exit(1);
    }
    let host = &args[1];
    let port = args[2].parse::<u16>().expect("Error parsing port");
    let address = &args[3];
    let message = Message::amqp_value(Value::String(args[4].to_string()));

    let opts = ConnectionOptions::new()
        .sasl_mechanism(SaslMechanism::Anonymous)
        .idle_timeout(Duration::from_secs(5));
    let net =
        mio::MioNetwork::connect(&format!("{}:{}", host, port)).expect("Error opening network");
    let transport = Transport::new(net, 1024);
    let connection = connect(transport, opts).expect("Error opening connection");

    let mut p = Poll::new().expect("Failed to create poll");
    let waker = Arc::new(
        Waker::new(p.registry(), Token(u32::MAX as usize)).expect("Failed to create waker"),
    );
    let handle = connection.handle(waker.clone());

    let id = Token(1024);
    connection.transport().network_mut().register(id, &mut p).unwrap();

    // p.registry().register(net, t, )

    let mut received = Vec::new();

    let mut i = 0;

    let mut events = Events::with_capacity(1024);
    loop {
        p.poll(&mut events, Some(Duration::from_secs(2))).unwrap();  // only one socket to poll on here
        println!("polled is empty {}, {:?}", events.is_empty(), events);
        let result = connection.process(&mut received);
        println!("received {:?}, result {:?}", received, result);
        if let Err(IoError(err)) = result {
            if err.kind() == ErrorKind::WouldBlock {
                // continue;
            }
        }
        if received.len() != 0 {
            println!("received {:?}", received);
            exit(0);
        }
        i +=1;
        if i == 10 {
            println!("sending open");
            handle.open(Open::new("aaaa")).unwrap();
            connection.flush().unwrap();
        }
    }


    // let driver = Arc::new(ConnectionDriver::new(
    //     handle,
    //     Duration::from_millis(50000),
    // ));

    // driver.open({
    //     let mut open = Open::new("exabmpleee");
    //     // open.hostname = Some(host.to_string());
    //     open.channel_max = Some(u16::MAX);
    //     open.idle_timeout = Some(50000);
    //     open
    // }).unwrap();

    // waker.wake().unwrap();

    // driver.
    // connection.flush().unwrap();

    // let future = async {
    //     loop {
    //         println!("awaiting");
    //         let frame = driver.recv().await.unwrap();
    //         println!("frame {:?}", frame);
    //     }
    // };
    //
    // block_on(future);


    // handle.open(Open::new("example-send-minimal")).unwrap();
    // handle
    //     .begin(0, Begin::new(0, u32::MAX, u32::MAX))
    //     .unwrap();
    // handle
    //     .attach(
    //         0,
    //         Attach::new(address, 0, LinkRole::Sender)
    //             .target(Target::new().address(address))
    //             .initial_delivery_count(0),
    //     )
    //     .unwrap();
    //
    // connection.flush().unwrap();

    // let mut done = false;
    // while !done {
    //     let mut received = Vec::new();
    //     let result = connection.process(&mut received);
    //     println!("received {:?}", received);
    //     match result {
    //         Ok(_) => {
    //             // handle.open(Open::new("example-send-minimal")).unwrap();
    //             for frame in received.drain(..) {
    //                 println!("received a frame {:?}", frame);
    //             }
    //         }
    //         Err(err) => {
    //             println!("not ok: {:?}", err);
    //             exit(1);
    //         }
    //     }
    // }


    // let mut buffer = Vec::new();
    // message.encode(&mut buffer).unwrap();
    // //
    // let mut done = false;
    // while !done {
    //     let mut received = Vec::new();
    //     let result = connection.process(&mut received);
    //     match result {
    //         Ok(_) => {
    //             println!("received {:?}", received);
    //             // handle.open(Open::new("example-send-minimal")).unwrap();
    //             for frame in received.drain(..) {
    //                 println!("received a frame {:?}", frame);
    //                 match frame {
    //                     Frame::AMQP(AmqpFrame {
    //                         channel: _,
    //                         performative,
    //                         payload: _,
    //                     }) => match performative {
    //                         Some(Performative::Flow(_)) => {
    //                             // handle
    //                             //     .transfer(
    //                             //         0,
    //                             //         Transfer::new(0)
    //                             //             .delivery_id(0)
    //                             //             .delivery_tag(&[1])
    //                             //             .settled(true),
    //                             //         Some(buffer.clone()),
    //                             //     )
    //                             //     .unwrap();
    //                             // handle.close(Close { error: None }).unwrap();
    //                         }
    //                         Some(Performative::Close(_)) => {
    //                             done = true;
    //                         }
    //                         _ => {}
    //                     },
    //
    //                     _ => {}
    //                 }
    //             }
    //         }
    //         _ => {}
    //     }
    // }
}
