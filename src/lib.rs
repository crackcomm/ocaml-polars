// Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

// for ocamlrep::ToOcamlRep derive macro
#![allow(non_local_definitions)]

mod blocking_section;
mod lazy;
pub mod polars0;
pub mod series_bigarray;
pub mod types;

use std::iter::FromIterator;
use std::sync::Arc;

use polars::prelude::{IntoLazy, IntoSeries, PolarsUpsample, SerReader};

use crate::blocking_section::releasing_runtime;
pub use crate::{series_bigarray::*, types::*};

#[ocaml::func]
pub fn ml_df_read_csv(filepath: String, opts: ReadCsv) -> Result<DataFramePtr, Error> {
    let schema = opts.schema.as_ref().map(|fields| {
        Arc::new(pl::Schema::from_iter(
            fields
                .into_iter()
                .map(|(name, dtype)| pl::Field::new(name, dtype.into())),
        ))
    });

    let df = releasing_runtime(move || {
        pl::CsvReader::from_path(&filepath)?
            .with_n_threads(Some(opts.n_threads))
            .has_header(opts.has_header)
            .with_skip_rows(opts.skip_rows)
            .with_dtypes(schema)
            .with_columns(opts.columns)
            .finish()
            .map_err(|e| error_with_desc(e, format!("read csv {}", filepath)))
    })?;

    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_df_write_parquet(df: DataFramePtr, filepath: String) -> Result<usize, Error> {
    let mut df = deref!(df).clone();
    releasing_runtime(move || {
        let f = std::fs::File::create(&filepath)?;
        pl::ParquetWriter::new(f)
            .set_parallel(false)
            .finish(&mut df)
            .map(|v| v as usize)
            .map_err(|e| error_with_desc(e, format!("write parquet {}", filepath)))
    })
}

#[ocaml::func]
pub unsafe fn ml_df_read_parquet(
    filepath: String,
    rechunk: bool,
    parallel: bool,
) -> Result<DataFramePtr, Error> {
    let df = releasing_runtime(move || {
        let f = std::fs::File::open(&filepath)?;
        pl::ParquetReader::new(f)
            .read_parallel(if parallel {
                pl::ParallelStrategy::Auto
            } else {
                pl::ParallelStrategy::None
            })
            .set_rechunk(rechunk)
            .finish()
            .map_err(|e| error_with_desc(e, format!("read parquet {}", filepath)))
    })?;
    Ok(Pointer::alloc_custom(df.into()))
}

/// Check if all values in DataFrames are equal where `None == None` evaluates to true.
#[ocaml::func]
pub fn ml_df_equal(df: DataFramePtr, other: DataFramePtr) -> bool {
    releasing_runtime(move || deref!(df).equals_missing(deref!(other)))
}

#[ocaml::func]
pub fn ml_df_print(df: DataFramePtr) {
    println!("{:?}", deref!(df));
}

#[ocaml::func]
pub fn ml_df_slice(df: DataFramePtr, offset: isize, length: usize) -> DataFramePtr {
    let df = deref!(df).slice(offset as i64, length);
    Pointer::alloc_custom(df.into())
}

#[ocaml::func]
pub fn ml_df_height(df: DataFramePtr) -> usize {
    deref!(df).height()
}

#[ocaml::func]
pub fn ml_df_width(df: DataFramePtr) -> usize {
    deref!(df).width()
}

#[ocaml::func]
pub fn ml_df_shape(df: DataFramePtr) -> (usize, usize) {
    deref!(df).shape()
}

#[ocaml::func]
pub fn ml_df_get_row(df: DataFramePtr, idx: usize) -> Result<Vec<AnyValue>, Error> {
    deref!(df)
        .get_row(idx)
        .map_err(|e| Error::Error(Box::new(e)))?
        .0
        .into_iter()
        .map(AnyValue::try_from)
        .collect()
}

#[ocaml::func]
pub fn ml_df_get(df: DataFramePtr, col: usize, idx: usize) -> Result<AnyValue, Error> {
    deref!(df)
        .select_at_idx(col)
        .ok_or_else(|| error_msg(format!("column {} out of bounds", col)))?
        .get(idx)?
        .try_into()
}

#[ocaml::func]
pub fn ml_df_get_by_name(df: DataFramePtr, name: &str, idx: usize) -> Result<AnyValue, Error> {
    let df = deref!(df);
    let col_idx = df
        .get_column_index(name)
        .ok_or_else(|| error_msg(format!("column {} not found", name)))?;

    df.select_at_idx(col_idx).unwrap().get(idx)?.try_into()
}

#[ocaml::func]
pub fn ml_df_select(df: DataFramePtr, columns: Vec<String>) -> Result<DataFramePtr, Error> {
    Ok(Pointer::alloc_custom(deref!(df).select(columns)?.into()))
}

#[ocaml::func]
pub fn ml_df_drop(df: DataFramePtr, columns: Vec<String>) -> Result<DataFramePtr, Error> {
    Ok(Pointer::alloc_custom(deref!(df).drop_many(&columns).into()))
}

#[ocaml::func]
pub fn ml_df_column_names(df: DataFramePtr) -> Vec<String> {
    deref!(df)
        .get_column_names()
        .into_iter()
        .map(|s| s.to_owned())
        .collect()
}

#[ocaml::func]
pub fn ml_df_rename_in_place(mut df: DataFramePtr, column: &str, name: &str) -> Result<(), Error> {
    let _ = deref_mut!(df).rename(column, name)?;
    Ok(())
}

#[ocaml::func]
pub fn ml_df_select_by_idx(df: DataFramePtr, col: usize) -> Option<SeriesPtr> {
    deref!(df)
        .select_at_idx(col)
        .map(|series| Pointer::alloc_custom(Series::from(series.clone())))
}

#[ocaml::func]
pub fn ml_df_select_by_name(df: DataFramePtr, name: &str) -> Option<SeriesPtr> {
    let df = deref!(df);
    df.get_column_index(name)
        .and_then(|idx| df.select_at_idx(idx))
        .map(|series| Pointer::alloc_custom(Series::from(series.clone())))
}

#[ocaml::func]
pub fn ml_df_with_column(df: DataFramePtr, series: SeriesPtr) -> Result<DataFramePtr, Error> {
    let mut df = deref!(df).clone();
    let _ = df.with_column(deref!(series).clone())?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_df_sort(
    df: DataFramePtr,
    by_column: Vec<String>,
    descending: bool,
    maintain_order: bool,
) -> Result<DataFramePtr, Error> {
    let mut df = deref!(df).clone();
    let opts = pl::SortMultipleOptions {
        descending: vec![descending],
        nulls_last: false,
        multithreaded: false,
        maintain_order,
    };
    releasing_runtime(|| df.sort_in_place(by_column, opts))?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_df_sort_in_place(
    mut df: DataFramePtr,
    by_column: Vec<String>,
    descending: bool,
    maintain_order: bool,
) -> Result<(), Error> {
    let df = deref_mut!(df);
    let opts = pl::SortMultipleOptions {
        descending: vec![descending],
        nulls_last: false,
        multithreaded: false,
        maintain_order,
    };
    releasing_runtime(move || df.sort_in_place(by_column, opts))?;
    Ok(())
}

#[ocaml::func]
pub fn ml_df_upsample(
    df: DataFramePtr,
    by: Option<Vec<String>>,
    time_column: String,
    every: Duration,
    offset: Duration,
) -> Result<DataFramePtr, Error> {
    let df = deref!(df).clone();
    let df = releasing_runtime(move || {
        df.upsample(
            by.unwrap_or(vec![]),
            &time_column,
            pl::Duration::from(&every),
            pl::Duration::from(&offset),
        )
    })?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_df_upsample_stable(
    df: DataFramePtr,
    by: Option<Vec<String>>,
    time_column: String,
    every: Duration,
    offset: Duration,
) -> Result<DataFramePtr, Error> {
    let df = deref!(df).clone();
    let df = releasing_runtime(move || {
        df.upsample_stable(
            by.unwrap_or(vec![]),
            &time_column,
            pl::Duration::from(&every),
            pl::Duration::from(&offset),
        )
    })?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_series_get(series: SeriesPtr, idx: usize) -> Result<AnyValue, Error> {
    deref!(series).get(idx)?.try_into()
}

#[ocaml::func]
pub fn ml_series_length(series: SeriesPtr) -> usize {
    deref!(series).len()
}

#[ocaml::func]
pub fn ml_series_name(series: SeriesPtr) -> String {
    deref!(series).name().to_owned()
}

#[ocaml::func]
pub fn ml_series_sum(series: SeriesPtr) -> f64 {
    deref!(series).sum().unwrap_or(0.0)
}

#[ocaml::func]
pub fn ml_series_cast(series: SeriesPtr, dtype: DataType) -> Result<SeriesPtr, Error> {
    let dtype = pl::DataType::from(&dtype);
    let series = deref!(series).cast(&dtype)?;
    Ok(Pointer::alloc_custom(series.into()))
}

#[ocaml::func]
pub fn ml_series_slice(series: SeriesPtr, offset: isize, length: usize) -> SeriesPtr {
    let series = deref!(series).slice(offset as i64, length);
    Pointer::alloc_custom(series.into())
}

#[ocaml::func]
pub fn ml_series_rechunk(series: SeriesPtr) -> SeriesPtr {
    let series = deref!(series).clone();
    let series = releasing_runtime(move || series.rechunk());
    Pointer::alloc_custom(series.into())
}

#[ocaml::func]
pub fn ml_series_null_count(series: SeriesPtr) -> usize {
    let series = deref!(series).clone();
    releasing_runtime(move || series.null_count())
}

#[ocaml::func]
pub fn ml_series_multiply(series: SeriesPtr, right: AnyValue) -> Result<SeriesPtr, Error> {
    let series = deref!(series);
    let series = match right {
        AnyValue::Int64(v) => Ok(series * v),
        AnyValue::Float32(v) => Ok(series * v),
        AnyValue::Float64(v) => Ok(series * v),
        AnyValue::Datetime(v, _) => Ok(series * v),
        AnyValue::Boolean(_) => Err(Error::Message(
            "the trait `NumCast` is not implemented for `bool`",
        )),
    }?;
    Ok(Pointer::alloc_custom(series.into()))
}

#[ocaml::func]
pub fn ml_series_set_sorted_flag(mut series: SeriesPtr, is_sorted: IsSorted) {
    deref_mut!(series).set_sorted_flag(pl::IsSorted::from(&is_sorted))
}

#[ocaml::func]
pub fn ml_df_filter_col_by_name(
    df: DataFramePtr,
    name: String,
    cmp: Comparison,
    value: AnyValue,
) -> Result<DataFramePtr, Error> {
    let df = deref!(df).clone();
    let df = releasing_runtime(move || {
        df.lazy()
            .filter(pl::binary_expr(pl::col(&name), cmp.into(), value.into()))
            .collect()
    })?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_df_filter_col_by_name_multi(
    df: DataFramePtr,
    filters: Vec<(String, Comparison, AnyValue)>,
) -> Result<DataFramePtr, Error> {
    let df = deref!(df).clone().lazy();
    let df = releasing_runtime(move || {
        filters
            .into_iter()
            .fold(df, |df, (name, cmp, value)| {
                df.filter(pl::binary_expr(pl::col(&name), cmp.into(), value.into()))
            })
            .collect()
    })?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_series_create(name: &str, data: AnyBigarray, copy: bool) -> SeriesPtr {
    let series = if copy {
        data.to_series_copy(name)
    } else {
        data.to_series(name)
    };
    Pointer::alloc_custom(series.into())
}

#[ocaml::func]
pub fn ml_df_create(series: Vec<(String, AnyBigarray)>, copy: bool) -> Result<DataFramePtr, Error> {
    let series = series
        .iter()
        .map(|(name, value)| {
            if copy {
                value.to_series_copy(name)
            } else {
                value.to_series(name)
            }
        })
        .collect();
    let df = pl::DataFrame::new(series)?;
    Ok(Pointer::alloc_custom(df.into()))
}

#[ocaml::func]
pub fn ml_series_bigarray(series: SeriesPtr) -> Result<AnyBigarray, Error> {
    series_to_bigarray(deref!(series))
}

#[ocaml::func]
pub fn ml_series_dtype(df: SeriesPtr) -> Result<DataType, Error> {
    deref!(df).dtype().try_into()
}

#[ocaml::func]
pub fn ml_df_series_bigarrays(df: DataFramePtr) -> Result<Vec<(String, AnyBigarray)>, Error> {
    deref!(df)
        .iter()
        .map(|series| series_to_bigarray(series).map(|ba| (series.name().to_string(), ba)))
        .collect()
}

// NOTE: not exported and not tested
#[ocaml::func]
pub unsafe fn ml_df_from_bigarray_f(
    columns: Vec<String>,
    arr: bigarray::Array2<f32>,
) -> Result<DataFramePtr, Error> {
    let arr = arr.view();
    let series = columns
        .iter()
        .enumerate()
        .map(|(col_idx, name)| {
            let col = arr.index_axis(ndarray::Axis(1), col_idx);
            let arr = col
                .as_slice()
                .ok_or_else(|| Error::Message("series not rechunked"))?;
            Ok(pl::ChunkedArray::<pl::Float32Type>::mmap_slice(name, arr).into_series())
        })
        .collect::<Result<Vec<pl::Series>, Error>>()?;

    let df = pl::DataFrame::new(series)?;
    Ok(Pointer::alloc_custom(df.into()))
}

// NOTE: not exported and not tested
#[ocaml::func]
pub unsafe fn ml_df_from_bigarray_c(
    columns: Vec<String>,
    arr: bigarray::Array2<f32>,
) -> Result<DataFramePtr, Error> {
    let arr = arr.view();
    let series = columns
        .iter()
        .enumerate()
        .map(|(col_idx, name)| {
            let col = arr.index_axis(ndarray::Axis(1), col_idx);
            let arr = col
                .as_slice()
                .ok_or_else(|| Error::Message("series not rechunked"))?;
            Ok(pl::ChunkedArray::<pl::Float32Type>::mmap_slice(name, arr).into_series())
        })
        .collect::<Result<Vec<pl::Series>, Error>>()?;

    let df = pl::DataFrame::new(series)?;
    Ok(Pointer::alloc_custom(df.into()))
}
