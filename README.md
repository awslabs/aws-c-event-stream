## AWS C Event Stream

C99 implementation of the vnd.amazon.event-stream content-type.

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

CMake 3.9+ is required to build.

`<install-path>` must be an absolute path in the following instructions.

#### Linux-Only Dependencies

If you are building on Linux, you will need to build aws-lc and s2n-tls first.

```
git clone git@github.com:awslabs/aws-lc.git
cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-lc/build --target install

git clone git@github.com:aws/s2n-tls.git
cmake -S s2n-tls -B s2n-tls/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build s2n-tls/build --target install
```

#### Building aws-c-event-stream and Remaining Dependencies

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-checksums.git
cmake -S aws-checksums -B aws-checksums/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-checksums/build --target install

git clone git@github.com:awslabs/aws-c-cal.git
cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-cal/build --target install

git clone git@github.com:awslabs/aws-c-io.git
cmake -S aws-c-io -B aws-c-io/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-io/build --target install

git clone git@github.com:awslabs/aws-c-event-stream.git
cmake -S aws-c-event-stream -B aws-c-event-stream/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-event-stream/build --target install
```

## Encoding

Event stream encoding provides bidirectional communication between a client and a server.

Each message consists of two sections: the prelude and the data. The prelude consists of:
1. The total byte length of the message
2. The combined byte length of all headers

The data section consists of:
1. Headers
2. Payload

Each section ends with a 4-byte big-endian CRC32 checksum. The message checksum is for both the prelude section and the data section.

Total message overhead, including the prelude and both checksums, is 16 bytes.

The following diagram shows the components that make up a message and a header. There are multiple headers per message.

![Encoding Diagram](docs/images/encoding.png)
