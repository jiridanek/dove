use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use futures::executor::block_on;
use tokio::{select, task};
use tokio::time::sleep;
use dove::conn::ConnectionOptions;
use dove::container::{Container, Delivery, Disposition, Message, SaslMechanism, Value};
use dove::error::AmqpError;
use dove::message::MessageProperties;
// Value shouldn't be exported
use dove::sasl::SaslMechanism::Plain;
use dove::url;
use dove::url::UrlScheme;

fn get_arg_str(name: &str) -> Option<String> {
    for arg in std::env::args() {
        let arr: Vec<&str> = arg.splitn(2, "=").collect();
        if arr.len() != 2 {
            continue;
        }
        let key = arr[0];
        let value = arr[1];
        if key == name {
            return Some(value.to_string());
        }
    }

    None
}

fn get_arg_int(name: &str) -> Option<i32> {
    for arg in std::env::args() {
        let arr: Vec<&str> = arg.splitn(2, "=").collect();
        if arr.len() != 2 {
            continue;
        }
        let key = arr[0];
        let value = arr[1];
        if key == name {
            return Some(value.parse().unwrap());
        }
    }

    None
}

fn mandatory<T>(value: Option<T>) -> T {
    value.expect("Mandatory argument must be specified")
}

async fn async_main() {
    let verbose = false;

    if verbose {
        for arg in std::env::args() {
            println!("arg, {}", arg);
        }

        let a = Value::Timestamp(44);
        if let Value::Timestamp(b) = a {
            println!("timestamp {}", b);
        }
    }
    //let b: u64 = a.into();

let connection_mode  :String = mandatory(get_arg_str("connection-mode")); // string   // 'client' or 'server'
let channel_mode     :String = mandatory(get_arg_str("channel-mode")); // string   // 'active' or 'passive'
let operation        :String = mandatory(get_arg_str("operation")); // string   // 'send' or 'receive'
let id               :String = mandatory(get_arg_str("id")); // string   // A unique identifier for the application
let host             :String = mandatory(get_arg_str("host")); // string   // The socket name
let port             :String = mandatory(get_arg_str("port")); // string   // The socket port (or '-')
let path             :String = mandatory(get_arg_str("path")); // string   // A named source or target for a message, often a queue
let duration         :i32 = mandatory(get_arg_int("duration")); // integer  // Run time in seconds; 0 means no limit
let count            :i32 = mandatory(get_arg_int("count")); // integer  // Number of messages to transfer; 0 means no limit
let rate             :i32 = mandatory(get_arg_int("rate")); // integer  // Target message rate; 0 to disable
let body_size        :i32 = mandatory(get_arg_int("body-size")); // integer  // Length of generated message body
let credit_window    :i32 = mandatory(get_arg_int("credit-window")); // integer  // Size of credit window to maintain
let transaction_size :i32 = mandatory(get_arg_int("transaction-size")); // integer  // Size of transaction batches; 0 means no transactions
let durable          :i32 = mandatory(get_arg_int("durable")); // integer  // 1 if messages are durable; 0 if non-durable

    if connection_mode != "client" {
        panic!("only client mode supported");
    }
    if channel_mode != "active" {
        panic!("what is meant by channel mode, actually?");
    }

    if verbose {
        println!("{}", connection_mode);
    }

    let url = url::Url{
        scheme: UrlScheme::AMQP,
        username: None,
        password: None,
        hostname: &host,
        port: port.parse().expect("port not valid number"),
        address: &path,
    };
    let opts = ConnectionOptions {
        username: url.username.map(|s| s.to_string()),
        password: url.password.map(|s| s.to_string()),
        sasl_mechanism: url.username.map_or(Some(SaslMechanism::Anonymous), |_| {
            Some(SaslMechanism::Plain)
        }),
        idle_timeout: Some(Duration::from_secs(5)),
    };

    let container = Container::new()
        .expect("unable to create container")
        .start();

// connect creates the TCP connection and sends OPEN frame.

        let connection = container
            .connect(format!("{}:{}", url.hostname, url.port), opts)
            .await
            .expect("connection not created");

// new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");

    if operation == "send" {

// new_sender creates the AMQP sender link.
        let sender = session
            .new_sender(&url.address)
            .await
            .expect("sender not created");

        // looks like i may send before I get credits from server, so, how do I implement more realistic simple sender?
        /*
        thread 'main' panicked at 'delivery not received: NotEnoughCreditsToSend(Message { header: Some(MessageHeader { durable: Some(false), priority: Some(4), ttl: None, first_acquirer: Some(false), delivery_count: Some(0) }), delivery_annotations: None, message_annotations: None, properties: Some(MessageProperties { message_id: Some(Ulong(0)), user_id: None, to: None, subject: None, reply_to: None, correlation_id: None, content_type: None, content_encoding: None, absolute_expiry_time: None, creation_time: None, group_id: None, group_sequence: None, reply_to_group_id: None }), application_properties: Some([(Symbol([83, 101, 110, 100, 84, 105, 109, 101]), Timestamp(1638870305528))]), body: AmqpValue(String("xx")), footer: None })', examples/arrow.rs:144:48
         */

        /*
        client does not let me set my own message id, that is probably not a problem...
         */

//  Send message and get delivery.
        // need to await deliveries concurrently, same way c does

        let data = "x".repeat(body_size as usize);

        let thence = std::time::Instant::now();
        let mut sent = 0;

        let mut deliveries: Vec<Pin<Box<dyn Future<Output=Result<Disposition, AmqpError>>>>> = Vec::new();
        loop {
            let sendtime = SystemTime::now().duration_since(UNIX_EPOCH).expect("failed to get system time").as_millis();
            let mut message = Message::amqp_value(Value::String(data.to_string()));
            message.application_properties = Some(vec![(Value::String(String::from("SendTime")), Value::Long(sendtime as i64))]);
            message.properties = Some(MessageProperties{
                // message_id: Some(Value::String(String::from("a") + &*sent.to_string())),
                message_id: Some(Value::String(sent.to_string())),
                user_id: None,
                to: None,
                subject: None,
                reply_to: None,
                correlation_id: Some(Value::String(sent.to_string())),
                content_type: None,
                content_encoding: None,
                absolute_expiry_time: None,
                creation_time: None,
                group_id: None,
                group_sequence: None,
                reply_to_group_id: None
            });
            // println!("messageid {:?}", message.properties);
            let delivery = sender.send(message); // .await.expect("delivery not received");
            // delivery.await.expect("delivery not received");
            deliveries.push(Box::pin(delivery));
            println!("{},{}", sent, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
            sent += 1;
            if verbose {
                println!("Message sent!");
            }

            if count > 0 && sent == count {
                break;
            }
            if duration != 0 && thence.elapsed() >= Duration::from_secs(duration as u64) {
                break;
            }
        }
        // let _ = futures::future::join_all(deliveries).await;

        // deliveries.reverse();
        for future in deliveries {
            future.await;
        }
    } else if operation == "receive" {
        let receiver = session
            .new_receiver(&url.address)
            .await
            .expect("receiver not created");

        let thence = Instant::now();
        loop {
            if duration != 0 {
                let elapsed = thence.elapsed();
                let remaining = Duration::from_secs(duration as u64) - elapsed;
                if remaining.as_secs() <= 0 {
                    break;
                }

                let recv = receiver.receive();
                let time = async_std::task::sleep(Duration::from_secs(1));

                select! {
                    result = recv => {
                        let msg: Delivery = result.expect("failed to get msg");
                        let message = msg.message();
                        let props: &Vec<(Value, Value)> = message.application_properties.as_ref().expect("no app properties");
                        let mut sendtime: u64 = 0;
                        for (key, value) in props {
                            if *key == Value::Symbol(Vec::from("SendTime")) {
                                if let Value::Timestamp(timevalue) = value {
                                    sendtime = *timevalue;
                                    break;
                                }
                            }
                            panic!("send time not found in message");
                        }

                        let ref id = message.properties.as_ref().expect("get properties").correlation_id.as_ref().expect("get msg corr id");
                        // println!("id is {:?}", id);
                        let mid = if let Value::String(msgid) = id { msgid } else { "" };

                        let receivetime = SystemTime::now().duration_since(UNIX_EPOCH).expect("get time").as_millis();
                        println!("{},{},{}", mid, sendtime, receivetime);


                        // filter(|tuple| {tuple.0 == "SendTime"}).first().expect("SendTime prop not found");
                        // let val = prop.1;
                    },
                    _ = time => {
                        // println!("timed out in select");  // this is clean exit, btw
                        break;
                    }
                }
            }


        }

    } else {
        panic!("Unknown operation");
    }
}

pub struct TimerFuture {
    started: Instant,
    duration: Duration,
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        TimerFuture {
            duration,
            started: Instant::now(),
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Look at the shared state to see if the timer has already completed.
        if self.started.elapsed() >= self.duration {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn main() {
    block_on(async_main())
}

// "connection-mode=client channel-mode=active operation=send id=quiver-sender-bbc11a1e scheme= host=127.0.0.1 port=5672 path=q0 duration=10 count=0 rate=0 body-size=100 credit-window=1000 transaction-size=0 durable=0"