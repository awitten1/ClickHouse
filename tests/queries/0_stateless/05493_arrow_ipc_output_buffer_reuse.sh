#!/usr/bin/env bash
# Tags: no-fasttest
# Requires pyarrow for independent IPC validation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

python3 - <<'PY'
import datetime
from decimal import Decimal
import os
from pathlib import Path
import shlex
import subprocess

import pyarrow as pa
import pyarrow.ipc as ipc

root = Path(os.environ['CLICKHOUSE_TMP']) / os.environ['CLICKHOUSE_TEST_UNIQUE_NAME']
root.mkdir(parents=True, exist_ok=True)
command = shlex.split(os.environ['CLICKHOUSE_LOCAL'])
case = 0
for fmt in ('ArrowStream', 'Arrow'):
    for codec in ('none', 'lz4_frame', 'zstd'):
        for dictionary in (0, 1):
            for strings in (0, 1):
                for rows in (0, 4097):
                    case += 1
                    sql = f"""
                    WITH concat('row-', toString(number), unhex('00'), repeat('é', number % 4)) AS value
                    SELECT number AS id, value AS s,
                        CAST(value AS Nullable(String)) AS all_valid,
                        if(number % 5 = 0, NULL, value) AS nullable_s,
                        materialize(CAST(NULL AS Nullable(String))) AS all_null,
                        materialize('') AS empty_s, 'constant' AS constant_s,
                        toDate('1970-01-01') + toIntervalDay(number % 1000) AS d,
                        toDate32('1960-01-01') + toIntervalDay(number % 1000) AS d32,
                        toDecimal64(toInt64(number) - 2000, 2) AS price,
                        [value, '', NULL] AS a,
                        CAST(tuple(value, number) AS Tuple(text String, k UInt64)) AS t,
                        toLowCardinality(toString(number % 37)) AS lc
                    FROM numbers({rows})
                    SETTINGS max_threads=1, max_block_size=17,
                        output_format_arrow_record_batch_size=0,
                        output_format_arrow_record_batch_size_bytes=0,
                        output_format_arrow_max_record_batch_size_bytes=128,
                        output_format_arrow_compression_method='{codec}',
                        output_format_arrow_low_cardinality_as_dictionary={dictionary},
                        output_format_arrow_string_as_string={strings}
                    FORMAT {fmt}
                    """
                    data = subprocess.check_output(command + ['--path', str(root / str(case)), '--query', sql])
                    source = pa.BufferReader(data)
                    reader = ipc.open_stream(source) if fmt == 'ArrowStream' else ipc.open_file(source)
                    batches = list(reader) if fmt == 'ArrowStream' else [reader.get_batch(i) for i in range(reader.num_record_batches)]
                    # More batches than input blocks proves that row ranges begin beyond zero.
                    assert rows == 0 or len(batches) > (rows + 16) // 17, len(batches)
                    table = pa.Table.from_batches(batches, schema=reader.schema)
                    table.validate(full=True)
                    assert table.num_rows == rows
                    convert = (lambda x: x) if strings else (lambda x: x.encode())
                    values = [convert(f'row-{i}\0' + 'é' * (i % 4)) for i in range(rows)]
                    expected = {
                        'id': list(range(rows)), 's': values, 'all_valid': values,
                        'nullable_s': [None if i % 5 == 0 else values[i] for i in range(rows)],
                        'all_null': [None] * rows, 'empty_s': [convert('')] * rows,
                        'constant_s': [convert('constant')] * rows,
                        'd': [datetime.date(1970, 1, 1) + datetime.timedelta(days=i % 1000) for i in range(rows)],
                        'd32': [datetime.date(1960, 1, 1) + datetime.timedelta(days=i % 1000) for i in range(rows)],
                        'price': [Decimal(i - 2000).quantize(Decimal('.01')) for i in range(rows)],
                        'a': [[values[i], convert(''), None] for i in range(rows)],
                        't': [{'text': values[i], 'k': i} for i in range(rows)],
                        'lc': [convert(str(i % 37)) for i in range(rows)],
                    }
                    for name, expected_values in expected.items():
                        assert table.column(name).to_pylist() == expected_values, (fmt, codec, dictionary, strings, rows, name)
                    assert pa.types.is_date32(table.schema.field('d').type)
                    assert pa.types.is_decimal128(table.schema.field('price').type)
                    assert table.schema.field('price').type.scale == 2
                    assert table.schema.field('all_valid').nullable
                    print(f'{fmt} {codec} dict={dictionary} strings={strings} rows={rows}: OK')
PY
