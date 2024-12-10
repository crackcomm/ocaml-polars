// Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
// SPDX-License-Identifier: MIT

use polars::prelude as pl;
use polars::prelude::IntoLazy;

use crate::blocking_section::releasing_runtime;
use crate::{deref, polars0::*, types::*};

fn build_exprs(exprs: &Vec<Expr>) -> Vec<pl::Expr> {
    exprs.iter().map(pl::Expr::from).collect::<Vec<_>>()
}

#[ocaml::func]
pub fn ml_print_expr(expr: Expr) {
    println!("{:?}", pl::Expr::from(&expr));
}

#[ocaml::func]
pub fn ml_lazy_frame(df: DataFramePtr) -> LazyFramePtr {
    Pointer::alloc_custom(LazyFrame(deref!(df).clone().lazy()))
}

#[ocaml::func]
pub fn ml_lazy_with_columns(df: LazyFramePtr, cols: Vec<Expr>) -> LazyFramePtr {
    let df = deref!(df).clone().with_columns(build_exprs(&cols));
    Pointer::alloc_custom(LazyFrame(df))
}

#[ocaml::func]
pub fn ml_lazy_groupby_agg(df: LazyFramePtr, groupby: Vec<Expr>, agg: Vec<Expr>) -> LazyFramePtr {
    let df = deref!(df)
        .clone()
        .group_by(build_exprs(&groupby))
        .agg(build_exprs(&agg));
    Pointer::alloc_custom(LazyFrame(df))
}

#[ocaml::func]
pub fn ml_lazy_sort(df: LazyFramePtr, by_column: &str, opts: SortMultipleOptions) -> LazyFramePtr {
    let df = deref!(df)
        .clone()
        .sort([by_column], pl::SortMultipleOptions::from(&opts));
    Pointer::alloc_custom(LazyFrame(df))
}

#[ocaml::func]
pub fn ml_lazy_select(df: LazyFramePtr, exprs: Vec<Expr>) -> LazyFramePtr {
    let df = deref!(df).clone().select(build_exprs(&exprs));
    Pointer::alloc_custom(LazyFrame(df))
}

#[ocaml::func]
pub fn ml_lazy_collect(df: LazyFramePtr) -> Result<DataFramePtr, Error> {
    let df = deref!(df).clone();
    let df = releasing_runtime(|| df.collect())?;
    Ok(Pointer::alloc_custom(DataFrame(df)))
}
