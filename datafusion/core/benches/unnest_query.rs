// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_use]
extern crate criterion;
use criterion::Criterion;

use parking_lot::Mutex;
use std::sync::Arc;

use tokio::runtime::Runtime;

extern crate arrow;
extern crate datafusion;

use arrow::{
    array::{ArrayRef, FixedSizeListBuilder, Float32Array, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

fn query(ctx: Arc<Mutex<SessionContext>>) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = rt.block_on(ctx.lock().table("t")).unwrap();
    rt.block_on(df.unnest_column("f32").unwrap().collect())
        .unwrap();
}

fn create_context(
    array_len: usize,
    batch_size: usize,
    no_nulls: bool,
) -> Result<Arc<Mutex<SessionContext>>> {
    //define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Float32, false),
        Field::new(
            "f32",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                2,
            ),
            true,
        ),
    ]));
    // define data.

    let batches = (0..array_len / batch_size)
        .map(|i| {
            let mut tags_builder = FixedSizeListBuilder::new(StringBuilder::new(), 2);

            for idx in 0..batch_size {
                if idx % 3 != 0 || no_nulls {
                    tags_builder
                        .values()
                        .append_value(format!("tag{}1", idx + 1));
                    tags_builder
                        .values()
                        .append_value(format!("tag{}2", idx + 1));
                    tags_builder.append(true);
                } else if !no_nulls {
                    tags_builder.values().append_null();
                    tags_builder.values().append_null();
                    tags_builder.append(false);
                }
            }

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float32Array::from(vec![i as f32; batch_size])),
                    Arc::new(tags_builder.finish()) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 1048576; // 2^20
    let batch_size = 512; // 2^9
    let ctx = create_context(array_len, batch_size, false).unwrap();
    c.bench_function("unnest_20_9", |b| b.iter(|| query(ctx.clone())));

    let batch_size = 512; // 2^9
    let ctx = create_context(array_len, batch_size, true).unwrap();
    c.bench_function("unnest_20_9_null", |b| b.iter(|| query(ctx.clone())));

    let batch_size = 4096; // 2^12
    let ctx = create_context(array_len, batch_size, false).unwrap();
    c.bench_function("unnest_20_12", |b| b.iter(|| query(ctx.clone())));

    let batch_size = 4096; // 2^12
    let ctx = create_context(array_len, batch_size, true).unwrap();
    c.bench_function("unnest_20_12_no_null", |b| b.iter(|| query(ctx.clone())));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
