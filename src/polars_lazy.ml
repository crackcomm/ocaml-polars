(* Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Polars
include Any_value

module When_then_otherwise = struct
  type when_ = When of expr
  and then_ = Then of when_ * expr

  let when_ expr = When expr
  let then_ expr when_ = Then (when_, expr)

  let otherwise otherwise = function
    | Then (When predicate, then_) ->
      Ternary { predicate; truthy = then_; falsy = otherwise }
  ;;
end

include When_then_otherwise

module Builder = struct
  let fn ?(cast_to_supertypes = false) collect_groups input function_ =
    Function { input; function_; options = { collect_groups; cast_to_supertypes } }
  ;;

  let map ?cast_to_supertypes = fn ?cast_to_supertypes ElementWise
  let apply expr = fn GroupWise [| expr |]

  let rolling
        ?(min_periods = 0)
        ?weights
        ?(center = false)
        ?by
        ?closed_window
        ~window
        op
        expr
    =
    let opts =
      { window_size = window; min_periods; weights; center; by; closed_window }
    in
    Rolling (expr, op, opts)
  ;;

  let not expr = map [| expr |] (Boolean Not)
  let is_null expr = map [| expr |] (Boolean IsNull)
  let is_not_null expr = map [| expr |] (Boolean IsNotNull)
  let is_finite expr = map [| expr |] (Boolean IsFinite)
  let is_infinite expr = map [| expr |] (Boolean IsInfinite)
  let is_nan expr = map [| expr |] (Boolean IsNan)
  let is_not_nan expr = map [| expr |] (Boolean IsNotNan)

  (* drop nans *)
  let drop_nans expr = apply expr DropNans

  (* filling *)
  let fill_nan ~with_ expr = when_ (is_nan expr) |> then_ with_ |> otherwise expr
  let fill_null ~with_ expr = map ~cast_to_supertypes:true [| expr; with_ |] FillNull
  let forward_fill ?limit expr = ForwardFill (expr, limit)

  (* rounding *)
  let floor expr = map [| expr |] Floor
  let ceil expr = map [| expr |] Ceil

  (* literal *)
  let null = Literal Null
  let lit lit = Literal lit
  let int64 v = Literal (Int64 v)
  let bool v = Literal (Boolean v)
  let float32 v = Literal (Float32 v)
  let float64 v = Literal (Float64 v)

  (* column *)
  let col column_name = Column column_name
  let alias name expr = Alias (expr, name)
  let cast ?(strict = false) data_type expr = Cast { expr; data_type; strict }
  let set_sorted_flag flag expr = apply expr (SetSortedFlag flag)
  let last expr = Agg (Last expr)
  let first expr = Agg (First expr)
  let max expr = Agg (Max { input = expr; propagate_nans = false })
  let min expr = Agg (Min { input = expr; propagate_nans = false })
  let sum expr = Agg (Sum expr)
  let select exprs = Columns exprs

  let sort_by ?(ascending = false) expr column_name =
    let sort_options =
      { descending = [| Stdlib.not ascending |]
      ; nulls_last = false
      ; maintain_order = false
      ; multithreaded = false
      }
    in
    SortBy { expr; by = [| col column_name |]; sort_options }
  ;;

  let collect expr = expr

  (* horizontal *)
  let horizontal op input = Horizontal { input; op }
  let min_horizontal = horizontal Min
  let max_horizontal = horizontal Max
  let sum_horizontal = horizontal Sum

  (* binary operators *)
  let binary op left right = BinaryExpr { left; op; right }
  let ( > ) = binary Gt
  let ( >= ) = binary GtEq
  let ( < ) = binary Lt
  let ( <= ) = binary LtEq
  let ( = ) = binary Eq
  let ( != ) = binary NotEq
  let ( || ) = binary Or
  let ( && ) = binary And
  let ( % ) = binary Modulus
  let plus = binary Plus
  let minus = binary Minus
  let multiply = binary Multiply
  let divide = binary Divide
  let true_divide = binary TrueDivide
  let floor_divide = binary FloorDivide
  let ( + ) = plus
  let ( - ) = minus
  let ( * ) = multiply
  let ( / ) = divide
  let ( // ) = floor_divide

  (* rolling *)
  let rolling_min = rolling Min
  let rolling_max = rolling Max
  let rolling_mean = rolling Mean
  let rolling_sum = rolling Sum
  let rolling_median = rolling Median
  let rolling_quantile q = rolling (Quantile q)
  let rolling_var = rolling Var
  let rolling_std = rolling Std

  (* cumulative *)
  let cumcount ?(reverse = false) expr = apply expr (CumCount { reverse })
  let cumsum ?(reverse = false) expr = apply expr (CumSum { reverse })
  let cumprod ?(reverse = false) expr = apply expr (CumProd { reverse })
  let cummin ?(reverse = false) expr = apply expr (CumMin { reverse })
  let cummax ?(reverse = false) expr = apply expr (CumMax { reverse })
end

include Builder

type t

external create : Polars.t -> t = "ml_lazy_frame"
external with_columns : t -> expr array -> t = "ml_lazy_with_columns"

let with_columns col t = with_columns t col
let with_column col = with_columns [| col |]

external groupby_agg : t -> expr array -> expr array -> t = "ml_lazy_groupby_agg"

let groupby_agg groupby agg t = groupby_agg t groupby agg

external sort : t -> string -> sort_multiple_options -> t = "ml_lazy_sort"

let sort
      ?(descending = false)
      ?(maintain_order = false)
      ?(nulls_last = false)
      ?(multithreaded = false)
      col
      t
  =
  let descending = [| descending |] in
  let opts = { descending; maintain_order; nulls_last; multithreaded } in
  sort t col opts
;;

external select : t -> expr array -> t = "ml_lazy_select"

let select exprs t = select t exprs

external collect : t -> Polars.t = "ml_lazy_collect"

module Eager = struct
  let select_names cols = Array.map ~f:col cols |> select

  let with_columns ?select exprs df =
    create df
    |> with_columns exprs
    |> Option.value_map ~default:Fn.id ~f:select_names select
    |> collect
  ;;

  let aggregate df exprs =
    let select = Array.map ~f:fst exprs in
    let exprs = Array.map ~f:(fun (name, expr) -> alias name expr) exprs in
    with_columns ~select exprs df
  ;;
end

external print_expr : expr -> unit = "ml_print_expr"
