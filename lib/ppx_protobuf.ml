open Ast_mapper
open Ast_helper
open Asttypes
open Parsetree
open Longident
open Location

let protobuf_mapper argv =
  default_mapper

let () = run_main protobuf_mapper
