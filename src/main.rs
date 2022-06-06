use proto::broker_proto::message_broker_server::MessageBrokerServer;
use tonic::transport::Server;
use log::info;

pub mod redis_client {
    use redis::AsyncCommands;
    use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
    use redis_async_pool::deadpool::managed::Object;
    use redis_async_pool::{RedisPool, RedisConnection};
    use redis::RedisError;

    #[derive(Clone)]
    pub struct RedisClient {
        pool: RedisPool
    }

    type ClientResult<T> = Result<T, RedisError>;
    type RedisPoolConn = Object<RedisConnection, RedisError>;

    impl RedisClient {
        pub fn new(pool: RedisPool) -> RedisClient {
            RedisClient { pool }
        }

        pub async fn get_connection_with_redis_error(&self) -> Result<RedisPoolConn, RedisError> {
            let conn = self.pool.get().await;
            match conn {
                Err(_) => {
                    Err(
                        RedisError::from((redis::ErrorKind::TryAgain, "Failed to establish connection."))
                    )
                },
                Ok(conn) => Ok(conn)
            }
        }

        pub async fn insert_into_stream<A: AsRef<str>, B: AsRef<str>>(
            &self, 
            stream_name: A,
            content: B,
            message_key: String,
            length_limit: Option<StreamMaxlen>
        ) -> ClientResult<String> {
            let mut conn = self.get_connection_with_redis_error().await?;

            let add_result = match length_limit {
                None => {
                    conn.xadd(stream_name.as_ref(), message_key, &[("message", content.as_ref())]).await
                },
                Some(opts) => {
                    conn.xadd_maxlen(
                        stream_name.as_ref(),
                        opts,
                        message_key,
                        &[("message", content.as_ref())]
                    ).await
                }
            }?;
            Ok(add_result)
        }

        pub async fn create_group<A: AsRef<str>, B: AsRef<str>, C: AsRef<str>>(
            &self,
            stream_name: A,
            group_name: B,
            message_key: C
        ) -> ClientResult<String> {
            let mut conn = self.get_connection_with_redis_error().await?;
            let response = conn.xgroup_create_mkstream(
                stream_name.as_ref(),
                group_name.as_ref(),
                message_key.as_ref()
            ).await?;
            Ok(response)      
        }

        pub async fn delete<A: AsRef<str>>(
            &self,
            key: A
        ) -> ClientResult<i32> {
            let mut conn = self.get_connection_with_redis_error().await?;
            let response = conn.del(
                key.as_ref()
            ).await?;
            Ok(response)
        }

        pub async fn read_messages<A: AsRef<str>, B: AsRef<str>>(
            &self,
            stream_name: A,
            message_key: B,
            opts: Option<StreamReadOptions>
        ) -> ClientResult<StreamReadReply> {
            let mut conn = self.get_connection_with_redis_error().await?;
            let response = match opts {
                None => conn.xread(
                    &[stream_name.as_ref()],
                    &[message_key.as_ref()]
                ).await,
                Some(opts) => conn.xread_options(
                    &[stream_name.as_ref()],
                    &[message_key.as_ref()],
                    opts
                ).await
            }?;
            Ok(response)
        }

        pub async fn delete_message_from_stream<A: AsRef<str>, B: AsRef<str>>(
            &self,
            stream_name: A,
            message_key: B
        ) -> ClientResult<i32> {
            let mut conn = self.get_connection_with_redis_error().await?;
            let response = conn.xdel(
                stream_name.as_ref(),
                    &[message_key.as_ref()]
            ).await?;
            Ok(response)
        }
    }
}


pub mod config {
    use serde::{Serialize, Deserialize};
    use serde::de::DeserializeOwned;
    use std::{
        fs::File,
        io::Read,
        collections::HashMap,
        env
    };
    use log::*;
    use serde_value::Value;

    #[derive(Debug, Clone)]
    pub struct Config {
        file: ConfigFile
    }
    
    type ConfigCreateResult = Result<Config, Box<dyn std::error::Error>>;

    impl Config {
        pub fn new<T: AsRef<str>>(path: T) -> ConfigCreateResult {
            let mut file = File::open(path.as_ref()).unwrap();
            let string = {
                let mut a = String::new();
                file.read_to_string(&mut a).unwrap();
                a
            };
            let file: ConfigFile = toml::from_str(string.as_str()).unwrap();
            Ok(Config { file })
        }

        pub fn get_variable<T: std::str::FromStr + DeserializeOwned, R: AsRef<str>>(&self, key: R) -> Option<T> {
            let mut value: Option<Value> = None;
            debug!("Finding value {} in env...", key.as_ref());
            if self.file.env_mapping.contains_key(key.as_ref()) {
                value = match env::var(
                    self.file.env_mapping.get(key.as_ref()).unwrap()
                ) {
                    Ok(value) => {
                        debug!("Found value {} in env!", key.as_ref());
                        Some(Value::String(value))   
                    },
                    Err(_) => None
                };
            }

            if value.is_none() {
                debug!("Finding value {} in config...", key.as_ref());
                let map = match serde_value::to_value(self.file.clone()) {
                    Ok(Value::Map(map)) => map,
                    _ => unimplemented!()
                };

                let map_key = Value::String(String::from(key.as_ref()));
                if map.contains_key(&map_key) {
                    debug!("Found value {} in config!", key.as_ref());
                    value = Some(map[&map_key].clone());
                }
            }

            debug!("Value {}: {:?}", key.as_ref(), value);
            let value: Value = value?;

            let deserialize_result = T::deserialize(value.clone());
            if let Ok(v) = deserialize_result {
                return Some(v)
            }

            warn!("Failed to deserialize {}", key.as_ref());
                    
            let string: String = match value {
                Value::String(v) => v,
                _ => return None
            };

            let str_value = string.as_str();
            if let Ok(v) = T::from_str(str_value) {
                debug!("Successfully parsed value from string!");
                return Some(v)
            } 
            None            
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct ConfigFile {
        service_ip: String,
        service_port: u32,

        redis_url: String,
        redis_pool_size: u32,

        log_level: String,
        default_block: u32,

        env_mapping: HashMap<String, String>
    }
}

pub mod redis_pool {
    use redis::Client;
    use redis::RedisError;
    use redis_async_pool::{RedisConnectionManager, RedisPool};
    
    pub fn get_connection_pool<T: AsRef<str>>(conn_params: T, connections_amount: usize) -> Result<RedisPool, RedisError> {
        let redis_conn = Client::open(conn_params.as_ref())?;
        let pool= RedisPool::new(
            RedisConnectionManager::new(redis_conn, true, None),
            connections_amount
        );
        Ok(pool)
    }
}

pub mod logger {
    use std::{env, io::Write};
    use chrono::Local;
    use log::info;

    pub fn init_logger(log_level: String) {
        env::set_var("RUST_LOG", log_level);
        env_logger::builder()
            .format_module_path(false)
            .format(
                |buf, record| {
                    writeln!(
                        buf,
                        "[{}:{} ] {}", Local::now().format("%d.%m.%Y %H:%M:%S.%s"), record.level(), record.args()
                    )
                }
            )
            .init();
        info!("Initialized logger...");
    }
}

pub mod grpc_service {
    use proto::broker_proto::{
        ReadReply, ReadRequest, InsertReply, InsertRequest,
        GroupCreateReply, GroupRequest, StreamRequest, StreamReply
    };
    use proto::broker_proto::message_broker_server::MessageBroker;
    use redis::streams::{StreamMaxlen, StreamReadOptions};
    use tonic::{Request, Response, Status};
    use crate::redis_client::RedisClient;
    use crate::config::Config;
    use log::error;

    pub struct BrokerService {
        client: RedisClient,
        config: Config
    }

    impl BrokerService {
        pub fn new(client: RedisClient, config: Config) -> Self {
            BrokerService { client, config }
        }

        fn generate_internal_error<T: Into<String>>(message: T) -> Status {
            Status::new(
                tonic::Code::Internal,
                message
            )
        }
    }

    #[tonic::async_trait]
    impl MessageBroker for BrokerService {
        async fn register_group(
            &self,
            request: Request<GroupRequest>,
        ) -> Result<Response<GroupCreateReply>, Status> {
            let group_info = request.into_inner();
            
            let message_key: String = match group_info.message_key {
                Some(key) => key,
                None => String::from("0")
            };

            let response = self.client.create_group(
                group_info.stream, 
                group_info.group_name, 
                message_key
            ).await;
            let created: bool = response.is_ok();
            let reply = match response {
                Ok(res) => res,
                Err(error) => error.to_string()
            };
            Ok(Response::new(GroupCreateReply{
                reply,
                created
            }))
        }

        async fn insert_message(
            &self,
            request: Request<InsertRequest>
        ) -> Result<Response<InsertReply>, Status> {
            let insert_info = request.into_inner();
            let mut opts: Option<StreamMaxlen> = None;
            if let Some(value) = insert_info.stream_length {
                opts = Some(StreamMaxlen::Approx(value as usize))
            }

            let message_key: String = match insert_info.message_key {
                None => String::from("*"),
                Some(value) => value
            };

            let add_result = self.client.insert_into_stream(
                insert_info.stream,
                insert_info.content,
                message_key,
                opts
            ).await;

            let response = match add_result {
                Ok(res) => InsertReply {
                    reply: res,
                    inserted: true
                },
                Err(error) => InsertReply {
                    reply: error.to_string(),
                    inserted: false
                }
            };
            Ok(Response::new(response))
        }

        async fn read_messages(
            &self,
            request: Request<ReadRequest>
        ) -> Result<Response<ReadReply>, Status> {
            let read_info = request.into_inner();

            let message_key = match read_info.message_key {
                None => String::from(">"),
                Some(value) => value
            };

            let block_time = match read_info.block_time {
                None => self.config.get_variable("default_block").unwrap(),
                Some(value) => value
            };

            let opts: StreamReadOptions = StreamReadOptions::default()
                .count(read_info.amount as usize)
                .group(read_info.group_name, read_info.consumer)
                .block(block_time as usize);
            
            let results = self.client.read_messages(
                read_info.stream.clone(),
                message_key,
                Some(opts)
            ).await;

            if let Err(error) = results {
                return Err(BrokerService::generate_internal_error(error.to_string()))
            }
            let results = results.unwrap();

            let mut messages: Vec<String> = Vec::new();

            if !results.keys.is_empty() {
                let results = results.keys[0].ids.clone();

                for res in results {
                    let del_res = self.client.delete_message_from_stream(
                        read_info.stream.clone(),
                        res.id
                    ).await;
                    if let Err(error) = del_res {
                        error!("Redis message delete error: {}", error)
                    };

                    let value = match res.map.get("message").unwrap() {
                        redis::Value::Data(vec) => Some(vec),
                        _ => None
                    };
                    let value = match value {
                        Some(val) => {
                            String::from_utf8(val.clone()).unwrap_or_else(|_| String::from(""))
                        },
                        None => String::from("")
                    };
                    messages.push(value);
                }
            }            

            Ok(Response::new(ReadReply { messages }))
        }

        async fn delete_stream(
            &self,
            request: Request<StreamRequest>
        ) -> Result<Response<StreamReply>, Status> {
            let delete_info = request.into_inner();

            let response = self.client.delete(
                delete_info.stream
            ).await;

            if let Err(error) = response {
                return Err(BrokerService::generate_internal_error(error.to_string()))
            }

            return Ok(Response::new(StreamReply {
                reply: response.unwrap()
            }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::Config::new("./config.toml").unwrap();
    logger::init_logger(config.get_variable("log_level").unwrap());

    let service_addr: String = {
        let ip: String = config.get_variable("service_ip").unwrap();
        let port: u32 = config.get_variable("service_port").unwrap();
        format!("{}:{}", ip, port)
    };

    info!("Starting server at {}", service_addr);
    let pool = {
        let url: String = config.get_variable("redis_url").unwrap();
        let redis_pool_size: u32 = config.get_variable("redis_pool_size").unwrap();
        redis_pool::get_connection_pool(url, redis_pool_size as usize).unwrap()
    };
    let client = redis_client::RedisClient::new(pool);
    let service = grpc_service::BrokerService::new(
        client,
        config
    );
    Server::builder()
        .add_service(MessageBrokerServer::new(service))
        .serve(service_addr.parse()?)
        .await?;

    Ok(())
}
