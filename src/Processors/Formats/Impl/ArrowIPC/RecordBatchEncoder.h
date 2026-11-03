#pragma once

#include "config.h"

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB::ArrowIPC
{

/// Encodes ClickHouse columns into the buffers of one Arrow IPC record batch, without the Apache
/// Arrow library. The result contains field nodes, buffer descriptors and an 8-byte-aligned body.
/// An uncompressed body can borrow ranges from source columns: the writer must substitute those ranges
/// for the corresponding uninitialized body bytes. Compressed bodies are contiguous and fully owned.
/// Supports flat and nested types; `LowCardinality` is materialized before encoding.
class RecordBatchEncoder
{
public:
    explicit RecordBatchEncoder(const FormatSettings & settings_) : settings(settings_) { }

    struct EncodedBatch
    {
        struct BorrowedBuffer
        {
            size_t offset;
            const char * data;
            size_t size;
        };

        VectorWithMemoryTracking<flatbuf::FieldNode> nodes;
        VectorWithMemoryTracking<flatbuf::Buffer> buffers;
        PODArray<char> body;
        VectorWithMemoryTracking<BorrowedBuffer> borrowed_buffers;
        /// Keep source and materialized columns alive until borrowed buffers have been written.
        Columns source_columns;
        Int64 num_rows = 0;
        /// Set when the body buffers were compressed; the writer then adds a BodyCompression to the batch.
        std::optional<CompressionCodec> codec;
    };

    /// Encodes rows [begin, end) of `columns`. A range rather than a whole column, so that a chunk too
    /// large for one record batch is written as several without copying a slice of it per batch.
    EncodedBatch encode(const Columns & columns, const DataTypes & types, size_t begin, size_t end);

    /// Return an already-written batch's allocations to the encoder for reuse.
    void recycle(EncodedBatch batch);

    /// Appends a buffer to the body (8-byte aligned start) and records its {offset, length}.
    void appendBuffer(const void * data, size_t length);
    /// Borrows source column data for uncompressed output, copying it for compressed output.
    void appendBorrowedBuffer(const void * data, size_t length);

private:
    char * allocateBuffer(size_t length);
    void encodeField(const IColumn & column, const DataTypePtr & type, size_t begin, size_t end);
    /// `null_map_column` (the outer `ColumnNullable`'s null map, or null for a non-nullable column) lets
    /// leaf encoders skip work for null rows — e.g. `DateTime64` rescaling, which would otherwise overflow
    /// on the arbitrary value a null row may carry.
    void encodeValues(
        const IColumn & column, const DataTypePtr & type, size_t begin, size_t end, const IColumn * null_map_column = nullptr);
    /// Encodes a Variant column as an Arrow dense union (no validity buffer; a types and an offsets
    /// buffer, the variant children in global order, and a trailing single-element null child).
    void encodeVariant(const IColumn & column, const DataTypePtr & type, size_t begin, size_t end);
    /// Writes a column with no first-class Arrow mapping as a variable-width Arrow column (an int32
    /// offsets buffer and the concatenated per-row serialized values). `arrowOpaqueValueIsText` chooses
    /// between `serializeText` and `serializeBinary`, and thereby between the `Utf8` and `Binary` Arrow
    /// types; `SchemaConverter::buildField` asks it the same question for the schema. Both Arrow types read
    /// back as `String`. `null_map_column` (when set) marks rows to emit as zero-length, so a NULL row's
    /// nested bytes are not written.
    void encodeAsOpaque(
        const IColumn & column,
        const DataTypePtr & type,
        size_t begin,
        size_t end,
        const IColumn * null_map_column = nullptr);

    void appendEmptyBuffer();
    /// Emits the validity buffer: a packed LSB-first bitmap (1 = valid) for nullable columns, or an
    /// empty buffer otherwise. Returns the null count.
    Int64 appendValidity(const IColumn * null_map_column, size_t begin, size_t end);
    /// Emits a `List`/`Map` offsets buffer, rebased so that the range's first row starts at 0.
    void appendOffsets(const IColumn::Offsets & ch_offsets, size_t begin, size_t end);

    const FormatSettings & settings;
    VectorWithMemoryTracking<flatbuf::FieldNode> nodes;
    VectorWithMemoryTracking<flatbuf::Buffer> buffers;
    PODArray<char> body;
    VectorWithMemoryTracking<EncodedBatch::BorrowedBuffer> borrowed_buffers;
    Columns source_columns;
};

}

#endif
