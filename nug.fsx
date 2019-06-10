#r "paket.exe"
// include Fake lib
#r "packages/FAKE/tools/FakeLib.dll"

open Fake
open Paket

let y = sprintf "%s/src/app/MasterDbLib.AzureTables.Lib" __SOURCE_DIRECTORY__

let dep = Dependencies.Locate(y).GetDirectDependencies()
System.IO.File.Delete("dep.txt")
let dep1 =  Seq.map (fun (x,y,z) -> System.IO.File.AppendAllLines("dep.txt",[sprintf "%s,%s" y z])) dep
List.ofSeq dep1
