use cdf_kernel::{CdfError, Result};

pub(crate) const WIRE_VARINT: u8 = 0;
pub(crate) const WIRE_FIXED64: u8 = 1;
pub(crate) const WIRE_LENGTH_DELIMITED: u8 = 2;
pub(crate) const WIRE_START_GROUP: u8 = 3;
pub(crate) const WIRE_END_GROUP: u8 = 4;
pub(crate) const WIRE_FIXED32: u8 = 5;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WireOccurrence<'a> {
    pub(crate) number: u32,
    pub(crate) wire_type: u8,
    pub(crate) value: &'a [u8],
    pub(crate) raw: &'a [u8],
    pub(crate) order: usize,
}

#[derive(Debug)]
pub(crate) struct MessageView<'a> {
    occurrences: Vec<WireOccurrence<'a>>,
    by_field: Vec<usize>,
}

impl<'a> MessageView<'a> {
    pub(crate) fn parse(bytes: &'a [u8], maximum_depth: u32) -> Result<Self> {
        Self::parse_many(std::iter::once(bytes), maximum_depth)
    }

    pub(crate) fn parse_many(
        messages: impl IntoIterator<Item = &'a [u8]>,
        maximum_depth: u32,
    ) -> Result<Self> {
        if maximum_depth == 0 {
            return Err(CdfError::contract(
                "Protobuf maximum_nesting_depth must be greater than zero",
            ));
        }
        let mut occurrences = Vec::with_capacity(16);
        for message in messages {
            parse_fields(message, 0, maximum_depth, &mut occurrences)?;
        }
        let mut by_field = (0..occurrences.len()).collect::<Vec<_>>();
        by_field.sort_unstable_by_key(|index| {
            let occurrence = &occurrences[*index];
            (occurrence.number, occurrence.order)
        });
        Ok(Self {
            occurrences,
            by_field,
        })
    }

    pub(crate) fn occurrences(&self) -> &[WireOccurrence<'a>] {
        &self.occurrences
    }

    pub(crate) fn field(&self, number: u32) -> impl Iterator<Item = &WireOccurrence<'a>> {
        let start = self
            .by_field
            .partition_point(|index| self.occurrences[*index].number < number);
        let end = self
            .by_field
            .partition_point(|index| self.occurrences[*index].number <= number);
        self.by_field[start..end]
            .iter()
            .map(|index| &self.occurrences[*index])
    }

    pub(crate) fn last_field_with_wire(
        &self,
        number: u32,
        wire_type: u8,
    ) -> Option<&WireOccurrence<'a>> {
        let start = self
            .by_field
            .partition_point(|index| self.occurrences[*index].number < number);
        let end = self
            .by_field
            .partition_point(|index| self.occurrences[*index].number <= number);
        self.by_field[start..end].iter().rev().find_map(|index| {
            let occurrence = &self.occurrences[*index];
            (occurrence.wire_type == wire_type).then_some(occurrence)
        })
    }
}

fn parse_fields<'a>(
    bytes: &'a [u8],
    depth: u32,
    maximum_depth: u32,
    output: &mut Vec<WireOccurrence<'a>>,
) -> Result<()> {
    if depth >= maximum_depth {
        return Err(CdfError::data(format!(
            "Protobuf message nesting exceeds the configured {maximum_depth}-level limit; increase format_options.maximum_nesting_depth only for a trusted producer"
        )));
    }
    let mut offset = 0;
    while offset < bytes.len() {
        let start = offset;
        let (tag, tag_bytes) = decode_varint(&bytes[offset..], "field tag")?;
        offset = checked_advance(offset, tag_bytes, bytes.len(), "field tag")?;
        let number = u32::try_from(tag >> 3)
            .map_err(|_| CdfError::data("Protobuf field number exceeds u32"))?;
        let wire_type = u8::try_from(tag & 0x07)
            .map_err(|_| CdfError::data("Protobuf wire type exceeds u8"))?;
        if number == 0 || number > 536_870_911 {
            return Err(CdfError::data(format!(
                "Protobuf field tag declares invalid field number {number}"
            )));
        }
        let value_start = offset;
        let value = match wire_type {
            WIRE_VARINT => {
                let (_, length) = decode_varint(&bytes[offset..], "varint field")?;
                offset = checked_advance(offset, length, bytes.len(), "varint field")?;
                &bytes[value_start..offset]
            }
            WIRE_FIXED64 => {
                offset = checked_advance(offset, 8, bytes.len(), "fixed64 field")?;
                &bytes[value_start..offset]
            }
            WIRE_LENGTH_DELIMITED => {
                let (length, length_bytes) =
                    decode_varint(&bytes[offset..], "length-delimited field size")?;
                offset = checked_advance(
                    offset,
                    length_bytes,
                    bytes.len(),
                    "length-delimited field size",
                )?;
                let payload_start = offset;
                let length = usize::try_from(length)
                    .map_err(|_| CdfError::data("Protobuf length-delimited field exceeds usize"))?;
                offset = checked_advance(
                    offset,
                    length,
                    bytes.len(),
                    "length-delimited field payload",
                )?;
                &bytes[payload_start..offset]
            }
            WIRE_START_GROUP => {
                let (inner, end) = find_group_end(bytes, offset, number, depth + 1, maximum_depth)?;
                offset = end;
                inner
            }
            WIRE_END_GROUP => {
                return Err(CdfError::data(format!(
                    "Protobuf message contains unmatched end-group tag for field {number}"
                )));
            }
            WIRE_FIXED32 => {
                offset = checked_advance(offset, 4, bytes.len(), "fixed32 field")?;
                &bytes[value_start..offset]
            }
            other => {
                return Err(CdfError::data(format!(
                    "Protobuf field {number} uses invalid wire type {other}"
                )));
            }
        };
        let order = output.len();
        output.push(WireOccurrence {
            number,
            wire_type,
            value,
            raw: &bytes[start..offset],
            order,
        });
    }
    Ok(())
}

fn find_group_end(
    bytes: &[u8],
    mut offset: usize,
    group_number: u32,
    depth: u32,
    maximum_depth: u32,
) -> Result<(&[u8], usize)> {
    if depth >= maximum_depth {
        return Err(CdfError::data(format!(
            "Protobuf group nesting exceeds the configured {maximum_depth}-level limit"
        )));
    }
    let payload_start = offset;
    loop {
        if offset == bytes.len() {
            return Err(CdfError::data(format!(
                "Protobuf group field {group_number} ended before its matching end tag"
            )));
        }
        let tag_start = offset;
        let (tag, tag_bytes) = decode_varint(&bytes[offset..], "group field tag")?;
        offset = checked_advance(offset, tag_bytes, bytes.len(), "group field tag")?;
        let number = u32::try_from(tag >> 3)
            .map_err(|_| CdfError::data("Protobuf group field number exceeds u32"))?;
        let wire_type = u8::try_from(tag & 0x07)
            .map_err(|_| CdfError::data("Protobuf group wire type exceeds u8"))?;
        if wire_type == WIRE_END_GROUP {
            if number != group_number {
                return Err(CdfError::data(format!(
                    "Protobuf group field {group_number} closed by end tag for field {number}"
                )));
            }
            return Ok((&bytes[payload_start..tag_start], offset));
        }
        offset = skip_value(bytes, offset, number, wire_type, depth, maximum_depth)?;
    }
}

fn skip_value(
    bytes: &[u8],
    offset: usize,
    number: u32,
    wire_type: u8,
    depth: u32,
    maximum_depth: u32,
) -> Result<usize> {
    match wire_type {
        WIRE_VARINT => {
            let (_, length) = decode_varint(&bytes[offset..], "group varint")?;
            checked_advance(offset, length, bytes.len(), "group varint")
        }
        WIRE_FIXED64 => checked_advance(offset, 8, bytes.len(), "group fixed64"),
        WIRE_LENGTH_DELIMITED => {
            let (length, length_bytes) =
                decode_varint(&bytes[offset..], "group length-delimited size")?;
            let payload = checked_advance(
                offset,
                length_bytes,
                bytes.len(),
                "group length-delimited size",
            )?;
            checked_advance(
                payload,
                usize::try_from(length).map_err(|_| {
                    CdfError::data("Protobuf group length-delimited field exceeds usize")
                })?,
                bytes.len(),
                "group length-delimited payload",
            )
        }
        WIRE_START_GROUP => {
            find_group_end(bytes, offset, number, depth + 1, maximum_depth).map(|(_, end)| end)
        }
        WIRE_END_GROUP => Err(CdfError::data(format!(
            "Protobuf group contains unmatched end tag for field {number}"
        ))),
        WIRE_FIXED32 => checked_advance(offset, 4, bytes.len(), "group fixed32"),
        other => Err(CdfError::data(format!(
            "Protobuf group field {number} uses invalid wire type {other}"
        ))),
    }
}

fn checked_advance(offset: usize, length: usize, total: usize, label: &str) -> Result<usize> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| CdfError::data(format!("Protobuf {label} length overflowed")))?;
    if end > total {
        return Err(CdfError::data(format!(
            "Protobuf {label} ended after {total} available bytes"
        )));
    }
    Ok(end)
}

pub(crate) fn decode_varint(bytes: &[u8], label: &str) -> Result<(u64, usize)> {
    let mut value = 0_u64;
    for index in 0..10 {
        let Some(&byte) = bytes.get(index) else {
            return Err(CdfError::data(format!(
                "Protobuf {label} ended inside a varint"
            )));
        };
        if index == 9 && byte > 1 {
            return Err(CdfError::data(format!(
                "Protobuf {label} exceeds the 64-bit varint encoding"
            )));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(CdfError::data(format!(
        "Protobuf {label} exceeds ten bytes"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arbitrary_wire_fields_and_preserves_raw_bytes() {
        let bytes = [0x08, 0x96, 0x01, 0x12, 0x03, b'a', b'b', b'c'];
        let view = MessageView::parse(&bytes, 32).unwrap();
        assert_eq!(view.occurrences().len(), 2);
        assert_eq!(view.occurrences()[0].raw, &bytes[..3]);
        assert_eq!(view.occurrences()[1].value, b"abc");
    }

    #[test]
    fn rejects_malformed_and_mismatched_groups() {
        let error = MessageView::parse(&[0x0b, 0x14], 32).unwrap_err();
        assert!(error.to_string().contains("closed by end tag for field 2"));
    }

    #[test]
    fn deterministic_malformed_corpus_never_panics_or_hangs() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for case in 0..4096_usize {
            let length = case % 129;
            let mut bytes = Vec::with_capacity(length);
            for _ in 0..length {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state as u8);
            }
            let _ = MessageView::parse(&bytes, 8);
        }
    }
}
