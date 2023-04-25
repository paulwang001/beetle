#FROM rust:latest AS builder
#WORKDIR /app
#COPY . .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
#COPY config $HOME/.cargo/config
# RUN cargo build --bin luffa-relay --release

FROM debian
WORKDIR /app
COPY ./luffa-relay/luffa.config.toml /root/.config/luffa/
COPY ./luffa-relay/luffa-relay .

CMD ["sh", "-c", "RUST_LOG=warn /app/luffa-relay --cfg /root/.config/luffa/luffa.config.toml"]