pub mod encoder;
pub mod decoder;
pub mod util;
pub mod message_codec;
pub mod simple;

pub fn encode_message(message: &::message::Message, buffer: &mut ::bytes::BytesMut) {
    ::codec::encoder::BinaryMessageEncoder::encode_message(message, buffer);
}

pub fn decode_message<B>(bytes: &mut B) -> ::message::Message
where
    B: ::bytes::Buf,
{
    ::codec::decoder::BinaryMessageDecoder::decode_message(bytes)
}
