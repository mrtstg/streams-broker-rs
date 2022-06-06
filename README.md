# streams-broker-rs
## About
streams-broker-rs - is a RPC server over redis streams, which allows to insert and read data from redis streams. You can see proto file [here](proto/broker.proto).
## How it works?
You can pass any string to broker using __InsertMessage__ method and it will insert into stream in format "message" - *your data*. Using __ReadMesssages__ method you can get array of strings from your string.
## Some details
- Stream reading is using __XGROUPREAD__ method, so using consumers is required. Server has __RegisterGroup__ method for creating groups and streams.
- You can pull ready image from [Docker hub](https://hub.docker.com/r/mrtstg/streams-broker-rs)
- Server tries to find file in parent folder, so in docker image volume file to __/usr/local/bin/__
## Config variables
Default config file is situated in "deployment" folder.

| Variable | Description |
| ---------|------------ |
| service_ip | Address, which RPC server will serve |
| service_port | Port, on which RPC server will be available |
| redis_url | Redis connection string |
| redis_pool_size | Amount of redis connections in pool |
| log_level | Logs level |
| default_block | Default block time (in ms) for XREAD operation |

Also, you can set any variable using virtual environment. For this, add field name and virtual environment variable name in [env_mapping] section.

For example, if we have line
```
log_level="LOG_LEVEL"
```
server will look up env. variable value "LOG_LEVEL" and if it doesn't exist will take variable from config file.
## TODO's
- [ ] Basic operations with keys
- [ ] Advanced message insert
- [ ] Consumer stats and management
- [ ] Changing config file path using env
