#FROM rust:latest AS builder
#WORKDIR /app
#COPY ../. .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
#COPY config $HOME/.cargo/config
#RUN cargo build --bin luffa-client --release

FROM debian
#ENV PARAMS=luffa.client.docker.toml
WORKDIR /app
COPY ./luffa-sdk/luffa.config.toml /root/.config/luffa/
COPY ./luffa-sdk/luffa-client .
CMD ["sh", "-c", "RUST_LOG=info ./luffa-client --tag Client-One --cfg /root/.config/luffa/luffa.config.toml"]