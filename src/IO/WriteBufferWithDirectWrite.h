#pragma once

#include <cstddef>

namespace DB
{

/// A write buffer that can send a block of bytes from the caller's memory instead of first copying it into its
/// own buffer. `writeDirect` has the effect of `write`: bytes already buffered go out first, and the block's
/// bytes count towards `count()`. Implementations may fall back to copying, for example for small blocks.
class IWriteBufferWithDirectWrite
{
public:
    virtual ~IWriteBufferWithDirectWrite() = default;
    virtual void writeDirect(const char * data, size_t size) = 0;
};

}
