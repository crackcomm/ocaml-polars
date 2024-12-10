// Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

use ocaml::{bigarray, Error, FromValue, ToValue};
use polars::{
    prelude as pl,
    prelude::{IntoSeries, NamedFrom},
};

#[derive(ToValue, FromValue)]
pub enum AnyBigarray {
    Bool(bigarray::Array1<u8>),
    Int64(bigarray::Array1<i64>),
    Float32(bigarray::Array1<f32>),
    Float64(bigarray::Array1<f64>),
}

impl AnyBigarray {
    pub fn to_series(&self, name: &str) -> pl::Series {
        match self {
            AnyBigarray::Bool(arr) => {
                // SAFETY: assert that the size of bool and u8 is the same
                assert_eq!(std::mem::size_of::<bool>(), std::mem::size_of::<u8>());
                pl::Series::new(name, unsafe {
                    core::slice::from_raw_parts(
                        arr.data().as_ptr() as *const _ as *const bool,
                        arr.len(),
                    )
                })
            }
            AnyBigarray::Int64(arr) => {
                assert_eq!(std::mem::size_of::<isize>(), std::mem::size_of::<i64>());
                unsafe {
                    pl::ChunkedArray::<pl::Int64Type>::mmap_slice(name, arr.data()).into_series()
                }
            }
            AnyBigarray::Float32(arr) => unsafe {
                pl::ChunkedArray::<pl::Float32Type>::mmap_slice(name, arr.data()).into_series()
            },
            AnyBigarray::Float64(arr) => unsafe {
                pl::ChunkedArray::<pl::Float64Type>::mmap_slice(name, arr.data()).into_series()
            },
        }
    }

    pub fn to_series_copy(&self, name: &str) -> pl::Series {
        match self {
            AnyBigarray::Bool(arr) => pl::Series::new(
                name,
                arr.data()
                    .iter()
                    .map(|&byte| byte != 0)
                    .collect::<Vec<bool>>(),
            ),
            AnyBigarray::Int64(arr) => pl::Series::new(name, arr.data()),
            AnyBigarray::Float32(arr) => pl::Series::new(name, arr.data()),
            AnyBigarray::Float64(arr) => pl::Series::new(name, arr.data()),
        }
    }
}

pub fn series_to_bigarray(series: &pl::Series) -> Result<AnyBigarray, Error> {
    match series.dtype() {
        pl::DataType::Boolean => {
            let chunk = series.bool()?;
            let mut arr = unsafe { bigarray::Array1::create(chunk.len()) };
            let values = arr.data_mut();
            // I think `mmap_slice` here is impossible because arrow uses bitmaps
            chunk
                .into_no_null_iter()
                .enumerate()
                .for_each(move |(idx, v)| {
                    values[idx] = v as u8;
                });
            Ok(AnyBigarray::Bool(arr))
        }
        pl::DataType::Int64 => {
            let chunk = series.i64()?.cont_slice()?;
            let arr = unsafe { bigarray::Array1::from_slice(chunk) };
            Ok(AnyBigarray::Int64(arr))
        }
        pl::DataType::Datetime(pl::TimeUnit::Milliseconds, _) => {
            let datetimes = series.datetime()?;
            let chunk = datetimes.0.cont_slice()?;
            let arr = unsafe { bigarray::Array1::from_slice(chunk) };
            Ok(AnyBigarray::Int64(arr))
        }
        pl::DataType::Float32 => {
            let chunk = series.f32()?.cont_slice()?;
            let arr = unsafe { bigarray::Array1::from_slice(chunk) };
            Ok(AnyBigarray::Float32(arr))
        }
        pl::DataType::Float64 => {
            let chunk = series.f64()?.cont_slice()?;
            let arr = unsafe { bigarray::Array1::from_slice(chunk) };
            Ok(AnyBigarray::Float64(arr))
        }
        _ => todo!(),
    }
}
