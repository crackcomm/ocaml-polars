(* Copyright © 2023 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Polars
open Polars_lazy

let%expect_test "print exprs" =
  print_expr (col "column_name");
  [%expect {| col("column_name") |}];
  print_expr (col "last_ts" |> set_sorted_flag Descending |> alias "timestamp");
  [%expect {| col("last_ts").set_sorted().alias("timestamp") |}];
  print_expr (col "column_name" // lit (Int64 42069));
  [%expect {| [(col("column_name")) floor_div (42069)] |}];
  print_expr (sum (col "price" * col "qty") / sum (col "qty") |> alias "vwap");
  [%expect
    {| [([(col("price")) * (col("qty"))].sum()) // (col("qty").sum())].alias("vwap") |}];
  print_expr (col "column_name" |> min);
  [%expect {| col("column_name").min() |}];
  print_expr (col "column_name" |> max);
  [%expect {| col("column_name").max() |}];
  print_expr (col "column_name" |> first);
  [%expect {| col("column_name").first() |}];
  print_expr (col "column_name" |> last);
  [%expect {| col("column_name").last() |}];
  print_expr (col "column_name" |> rolling_sum ~window:(Slots 10) |> alias "sum");
  [%expect {| col("column_name").rolling_sum().alias("sum") |}]
;;

let group ~interval =
  col "timestamp" // lit (Int64 interval) |> set_sorted_flag Descending |> alias "group"
;;

let aggregate_trades ~interval df =
  let iv = lit (Float64 (Float.of_int interval)) in
  Polars_lazy.create df
  |> with_column (group ~interval)
  |> groupby_agg
       [| col "group" |]
       [| ceil (last (col "timestamp") / iv) * iv |> cast Int64
        ; first (col "price") |> alias "open"
        ; max (col "price") |> alias "high"
        ; min (col "price") |> alias "low"
        ; last (col "price") |> alias "close"
        ; sum (col "qty") |> alias "volume"
        ; sum (col "price" |> multiply (col "qty")) / sum (col "qty") |> alias "vwap"
       |]
  |> sort "group"
  |> select
       [| col "timestamp" |> set_sorted_flag Descending
        ; col "open"
        ; col "high"
        ; col "low"
        ; col "close"
        ; col "volume"
        ; col "vwap"
       |]
  |> collect
;;

let%expect_test "aggregate trades" =
  let df = Polars_testdata.trades1 () in
  Polars.print (aggregate_trades df ~interval:100);
  [%expect
    {|
    shape: (186, 7)
    | timestamp     | open         | high         | low          | close        | volume | vwap         |
    | ---           | ---          | ---          | ---          | ---          | ---    | ---          |
    | i64           | f32          | f32          | f32          | f32          | f32    | f32          |
    |---------------|--------------|--------------|--------------|--------------|--------|--------------|
    | 1694217604200 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.800781 | 1.13   | 25899.802734 |
    | 1694217604300 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.800781 | 2.97   | 25899.876953 |
    | 1694217604400 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.900391 | 11.078 | 25899.900391 |
    | 1694217604500 | 25899.800781 | 25899.900391 | 25899.800781 | 25899.900391 | 0.018  | 25899.832031 |
    | …             | …            | …            | …            | …            | …      | …            |
    | 1694217660100 | 25900.099609 | 25900.099609 | 25900.099609 | 25900.099609 | 0.001  | 25900.099609 |
    | 1694217660300 | 25900.0      | 25900.0      | 25900.0      | 25900.0      | 0.007  | 25900.0      |
    | 1694217660400 | 25900.099609 | 25900.099609 | 25900.099609 | 25900.099609 | 0.004  | 25900.099609 |
    | 1694217660600 | 25900.099609 | 25900.099609 | 25900.099609 | 25900.099609 | 0.016  | 25900.099609 | |}]
;;

let%expect_test "upsample" =
  let df = Polars_testdata.trades1 () |> aggregate_trades ~interval:100 in
  let series = Polars.select_by_name_exn df ~col:"timestamp" in
  let series = Series.cast ~dtype:(Datetime Milliseconds) series in
  Series.set_sorted_flag ~flag:Descending series;
  let df = Polars.with_column df ~col:series in
  let df =
    Polars.upsample
      ~maintain_order:true
      ~time_column:"timestamp"
      ~every:(Duration.of_int_ms 100)
      df
  in
  Polars.print df;
  [%expect
    {|
    shape: (565, 7)
    | timestamp               | open         | high         | low          | close        | volume | vwap         |
    | ---                     | ---          | ---          | ---          | ---          | ---    | ---          |
    | datetime[ms]            | f32          | f32          | f32          | f32          | f32    | f32          |
    |-------------------------|--------------|--------------|--------------|--------------|--------|--------------|
    | 2023-09-09 00:00:04.200 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.800781 | 1.13   | 25899.802734 |
    | 2023-09-09 00:00:04.300 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.800781 | 2.97   | 25899.876953 |
    | 2023-09-09 00:00:04.400 | 25899.900391 | 25899.900391 | 25899.800781 | 25899.900391 | 11.078 | 25899.900391 |
    | 2023-09-09 00:00:04.500 | 25899.800781 | 25899.900391 | 25899.800781 | 25899.900391 | 0.018  | 25899.832031 |
    | …                       | …            | …            | …            | …            | …      | …            |
    | 2023-09-09 00:01:00.300 | 25900.0      | 25900.0      | 25900.0      | 25900.0      | 0.007  | 25900.0      |
    | 2023-09-09 00:01:00.400 | 25900.099609 | 25900.099609 | 25900.099609 | 25900.099609 | 0.004  | 25900.099609 |
    | 2023-09-09 00:01:00.500 | null         | null         | null         | null         | null   | null         |
    | 2023-09-09 00:01:00.600 | 25900.099609 | 25900.099609 | 25900.099609 | 25900.099609 | 0.016  | 25900.099609 | |}];
  let agg = Eager.aggregate df [| "qty", col "close" |> forward_fill |] in
  Polars.print agg;
  [%expect
    {|
    shape: (565, 1)
    | qty          |
    | ---          |
    | f32          |
    |--------------|
    | 25899.800781 |
    | 25899.800781 |
    | 25899.900391 |
    | 25899.900391 |
    | …            |
    | 25900.0      |
    | 25900.099609 |
    | 25900.099609 |
    | 25900.099609 | |}]
;;

let%expect_test "rolling sum" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg = Eager.aggregate df [| "sum", col "qty" |> rolling_sum ~window:(Slots 5) |] in
  Polars_testing.print_col_f32 agg ~col:"sum";
  [%expect
    {| [0.0050 1.0250 1.0440 1.1300 3.2730 3.2690 2.2500 2.9030 2.9510 0.8180 0.8180 0.8250 0.1570 0.0240 11.0870 11.0870 11.0810 11.0880 11.0910 0.0190 0.0210 0.0200 0.0460 0.0550 0.1360 0.1410 0.1780 0.1790 0.2630 0.2110 0.6700 1.5670 1.7190 3.4020 4.0960 9.1460 9.1610 9.1260 7.3480 6.6350] |}]
;;

let%expect_test "rolling max" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg = Eager.aggregate df [| "max", col "qty" |> rolling_max ~window:(Slots 5) |] in
  Polars_testing.print_col_f32 agg ~col:"max";
  [%expect
    {| [0.0050 1.0200 1.0200 1.0200 2.1430 2.1430 2.1430 2.1430 2.1430 0.6720 0.6720 0.6720 0.1340 0.0100 11.0730 11.0730 11.0730 11.0730 11.0730 0.0110 0.0110 0.0110 0.0370 0.0370 0.0820 0.0820 0.0820 0.0820 0.0970 0.0970 0.4670 0.9350 0.9350 1.7800 1.7800 5.5170 5.5170 5.5170 5.5170 5.5170] |}]
;;

let%expect_test "max_horizontal" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg =
    Eager.aggregate df [| "max", max_horizontal [| col "first_id"; col "last_id" |] |]
  in
  Polars_testing.print_col_i64 agg ~col:"max";
  [%expect
    {| [4071974096 4071974098 4071974099 4071974101 4071974104 4071974105 4071974106 4071974109 4071974113 4071974114 4071974115 4071974117 4071974118 4071974119 4071974124 4071974125 4071974126 4071974131 4071974133 4071974134 4071974136 4071974137 4071974139 4071974140 4071974141 4071974142 4071974143 4071974144 4071974146 4071974147 4071974148 4071974150 4071974151 4071974154 4071974156 4071974171 4071974172 4071974179 4071974180 4071974181] |}]
;;

let%expect_test "cummax" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg = Eager.aggregate df [| "qty", col "qty" |> cummax |] in
  Polars_testing.print_col_f32 agg ~col:"qty";
  [%expect
    {| [0.0050 1.0200 1.0200 1.0200 2.1430 2.1430 2.1430 2.1430 2.1430 2.1430 2.1430 2.1430 2.1430 2.1430 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730 11.0730] |}]
;;

let%expect_test "cummin" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg = Eager.aggregate df [| "qty", col "qty" |> cummin |] in
  Polars_testing.print_col_f32 agg ~col:"qty";
  [%expect
    {| [0.0050 0.0050 0.0050 0.0050 0.0050 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010 0.0010] |}]
;;

let%expect_test "cumsum" =
  let df = Polars_testdata.trades1 () |> Polars.sub ~pos:0 ~len:40 in
  let agg = Eager.aggregate df [| "qty", col "qty" |> cumsum |] in
  Polars_testing.print_col_f32 agg ~col:"qty";
  [%expect
    {| [0.0050 1.0250 1.0440 1.1300 3.2730 3.2740 3.2750 3.9470 4.0810 4.0910 4.0920 4.1000 4.1040 4.1050 15.1780 15.1790 15.1810 15.1920 15.1960 15.1970 15.2000 15.2010 15.2380 15.2510 15.3330 15.3410 15.3790 15.4170 15.5140 15.5440 16.0110 16.9460 17.1360 18.9160 19.6400 25.1570 26.1070 26.2620 26.2640 26.2750] |}]
;;
