// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::io::prelude::*;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

const PACKET_BUFFER_SIZE: usize = 4_096;
const PACKET_LARGE_BUFFER_SIZE: usize = 1_048_576;

pub struct PacketReader<R> {
    bytes: Vec<u8>,
    start: usize,
    remaining: usize,
    pub r: R,
}

impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        PacketReader {
            bytes: Vec::new(),
            start: 0,
            remaining: 0,
            r,
        }
    }
}

impl<R: Read> PacketReader<R> {
    #[allow(dead_code)]
    pub fn next(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        self.start = self.bytes.len() - self.remaining;

        loop {
            if self.remaining != 0 {
                let bytes = {
                    // NOTE: this is all sorts of unfortunate. what we really want to do is to give
                    // &self.bytes[self.start..] to `packet()`, and the lifetimes should all work
                    // out. however, without NLL, borrowck doesn't realize that self.bytes is no
                    // longer borrowed after the match, and so can be mutated.
                    let bytes = &self.bytes[self.start..];
                    unsafe { ::std::slice::from_raw_parts(bytes.as_ptr(), bytes.len()) }
                };

                match packet(bytes) {
                    Ok((rest, p)) => {
                        self.remaining = rest.len();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        let err = Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                        self.bytes.truncate(self.remaining);
                        return err;
                    }
                }
            }

            // we need to read some more
            self.bytes.drain(0..self.start);
            self.start = 0;
            let end = self.bytes.len();
            self.bytes.resize(std::cmp::max(4096, end * 2), 0);
            let read = {
                let buf = &mut self.bytes[end..];
                self.r.read(buf)?
            };
            self.bytes.truncate(end + read);
            self.remaining = self.bytes.len();

            if read == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PacketReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self.remaining != 0 {
            buf.put_slice(&self.bytes[self.start..]);
            self.bytes.clear();
            self.start = 0;
            self.remaining = 0;
            std::task::Poll::Ready(Ok(()))
        } else {
            std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }
}

impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next_async(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        self.start = self.bytes.len() - self.remaining;

        let mut buffer_size = PACKET_BUFFER_SIZE;
        loop {
            if self.remaining != 0 {
                let bytes = {
                    // NOTE: this is all sorts of unfortunate. what we really want to do is to give
                    // &self.bytes[self.start..] to `packet()`, and the lifetimes should all work
                    // out. however, without NLL, borrowck doesn't realize that self.bytes is no
                    // longer borrowed after the match, and so can be mutated.
                    let bytes = &self.bytes[self.start..];
                    unsafe { ::std::slice::from_raw_parts(bytes.as_ptr(), self.remaining) }
                };
                match packet(bytes) {
                    Ok((rest, p)) => {
                        // Only record how many bytes are left; do NOT reallocate
                        // `self.bytes` here. `p` (and `rest`) borrow into the
                        // current `self.bytes` allocation via the unsafe slice
                        // above, so replacing it (e.g. `self.bytes = rest.to_vec()`)
                        // would free the buffer `p` points into -> use-after-free,
                        // returning garbage to the caller for the *next* pipelined
                        // packet (e.g. mysqlnd sending EXECUTE+CLOSE+PREPARE
                        // back-to-back). The next call recomputes `self.start`
                        // from `len - remaining`, matching the sync `next()`.
                        self.remaining = rest.len();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        self.bytes.truncate(self.remaining);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                    }
                }
            }

            // we need to read some more
            self.bytes.drain(0..self.start);
            self.start = 0;
            let end = self.remaining;

            if self.bytes.len() - end < buffer_size {
                let new_len = std::cmp::max(buffer_size, end * 2);
                self.bytes.resize(new_len, 0);
            }
            let read = {
                let buf = &mut self.bytes[end..];
                self.r.read(buf).await?
            };
            self.remaining = end + read;
            // Drop the zero-padding added by `resize` so `self.bytes.len()`
            // always equals the number of real buffered bytes. The top-of-call
            // `self.start = self.bytes.len() - self.remaining` (and the borrow of
            // `self.bytes[self.start..]`) rely on this; leaving padding in made
            // `start` point into zeros after a pipelined packet was parsed.
            self.bytes.truncate(self.remaining);
            // use a larger buffer size to reduce bytes resize times.
            buffer_size = PACKET_LARGE_BUFFER_SIZE;

            if read == 0 {
                self.bytes.truncate(self.remaining);
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

pub fn fullpacket(i: &[u8]) -> nom::IResult<&[u8], (u8, &[u8])> {
    let (i, _) = nom::bytes::complete::tag(&[0xff, 0xff, 0xff])(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(U24_MAX)(i)?;
    Ok((i, (seq[0], bytes)))
}

pub fn onepacket(i: &[u8]) -> nom::IResult<&[u8], (u8, &[u8])> {
    let (i, length) = nom::number::complete::le_u24(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(length)(i)?;
    Ok((i, (seq[0], bytes)))
}

// Clone because of https://github.com/Geal/nom/issues/1008
#[derive(Clone)]
pub struct Packet<'a>(&'a [u8], Vec<u8>);

impl<'a> Packet<'a> {
    fn extend(&mut self, bytes: &'a [u8]) {
        if self.0.is_empty() {
            if self.1.is_empty() {
                // first extend
                self.0 = bytes;
            } else {
                // later extend
                self.1.extend(bytes);
            }
        } else {
            assert!(self.1.is_empty());
            let mut v = self.0.to_vec();
            v.extend(bytes);
            self.1 = v;
            self.0 = &[];
        }
    }
}

impl<'a> AsRef<[u8]> for Packet<'a> {
    fn as_ref(&self) -> &[u8] {
        if self.1.is_empty() {
            self.0
        } else {
            &self.1
        }
    }
}

use crate::U24_MAX;
use std::ops::Deref;

impl<'a> Deref for Packet<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub(crate) fn packet(i: &[u8]) -> nom::IResult<&[u8], (u8, Packet<'_>)> {
    nom::combinator::map(
        nom::sequence::pair(
            nom::multi::fold_many0(
                fullpacket,
                || (0, None),
                |(seq, pkt): (_, Option<Packet<'_>>), (nseq, p)| {
                    let pkt = if let Some(mut pkt) = pkt {
                        assert_eq!(nseq, seq + 1);
                        pkt.extend(p);
                        Some(pkt)
                    } else {
                        Some(Packet(p, Vec::new()))
                    };
                    (nseq, pkt)
                },
            ),
            onepacket,
        ),
        move |(full, last)| {
            let seq = last.0;
            let pkt = if let Some(mut pkt) = full.1 {
                assert_eq!(last.0, full.0 + 1);
                pkt.extend(last.1);
                pkt
            } else {
                Packet(last.1, Vec::new())
            };
            (seq, pkt)
        },
    )(i)
}

#[cfg(test)]
mod reader_tests {
    use super::PacketReader;

    fn frame(seq: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
        v.push(seq);
        v.extend_from_slice(payload);
        v
    }

    /// Several packets arriving in a single read (pipelined, as mysqlnd sends
    /// EXECUTE/CLOSE/PREPARE back-to-back) must each be returned intact. This
    /// guards the packet-reader buffer management against the use-after-free /
    /// zero-padding regressions that corrupted every packet after the first.
    #[tokio::test]
    async fn reads_pipelined_packets_intact() {
        let mut buf = Vec::new();
        buf.extend(frame(0, &[0x16, b'S', b'E', b'L'])); // prepare-ish
        buf.extend(frame(0, &[0x19, 1, 0, 0, 0])); // close stmt 1
        buf.extend(frame(0, &[0x17, 2, 0, 0, 0])); // execute stmt 2
        buf.extend(frame(0, &[0x01])); // quit

        let src: &[u8] = &buf;
        let mut r = PacketReader::new(src);

        let expect: [&[u8]; 4] = [
            &[0x16, b'S', b'E', b'L'],
            &[0x19, 1, 0, 0, 0],
            &[0x17, 2, 0, 0, 0],
            &[0x01],
        ];
        for want in expect {
            let (_seq, pkt) = r.next_async().await.unwrap().expect("a packet");
            assert_eq!(pkt.as_ref(), want, "pipelined packet payload mismatch");
        }
        assert!(
            r.next_async().await.unwrap().is_none(),
            "expected end of stream"
        );
    }
}
