// include Fake lib
#r "packages/FAKE/tools/FakeLib.dll"
#r "packages/Fake.DotNet.NuGet/lib/net46/Fake.DotNet.NuGet.dll"
#r "System.Xml.Linq"

open Fake
open System.Xml.Linq
open Fake.DotNet.NuGet

let authors = ["author"]
let projectName = "MasterDbLib.AzureTables.Lib.dll"
let projectSummary = "Summary"
let projectDescription = "Summary"
let myAccessKey = ""

RestorePackages()

// Properties
let buildDir = "./build/app/"
let buildForNugetFiles = buildDir + "*.dll"
//let testDir  = "./build/test/"
let deployDir = "./build/deploy/project/"
let deployNugetDir = @"./build/deploy/nuget/"
let nuspec = "app.nuspec"
let testPathPattern = buildDir + "/*.Tests.dll"
// version info
let version = "0.0.1"  // or retrieve from CI server


// Targets
Target "Clean" (fun _ ->
    CleanDirs [buildDir;  deployDir; deployNugetDir]
)

Target "BuildApp" (fun _ ->
    !! "src/app/*/*.csproj"
        |> MSBuildRelease buildDir "Build"
        |> Log "AppBuild-Output: "
)

(*
Target "BuildTest" (fun _ ->
    !! "src/test/**/*.csproj"
        |> MSBuildDebug testDir "Build"
        |> Log "TestBuild-Output: "
)
*)

Target "Test" (fun _ ->
    !! (testPathPattern)
        |> NUnit (fun p ->
            {p with
                DisableShadowCopy = true;
                OutputFile = buildDir + "TestResults.xml" })
)

Target "TestParallel" (fun _ ->
    !! (testPathPattern)
        |> NUnitParallel (fun p ->
            {p with
                DisableShadowCopy = true;
                OutputFile = buildDir + "TestResults.xml" })
)

//nug.fsx interface
let depends = System.IO.File.ReadAllLines("dep.txt") 
                    |> Seq.map(fun m -> m.Split(','))
                    |> Seq.map(fun m -> (m.[0],m.[1]))
                    |> List.ofSeq

Target "BuildNuGet" (fun _ ->   
    CopyTo deployNugetDir [buildDir]
    NuGet (fun p ->
    { p with
        Version = version
        Authors = authors
        Project = projectName
        Summary = projectSummary
        Description = projectDescription
        WorkingDir = buildDir
        OutputPath = deployNugetDir
        AccessKey = myAccessKey        
        Dependencies = depends
        DependenciesByFramework = [
        ]
        Publish = false 
        Files = [
            (buildForNugetFiles,Some "lib" ,None)
        ]
    }) "app.nuspec"
)
//The new Deploy target scans the build directory for all files. 
//The result will be zipped to /deploy/Calculator.zip via the Zip task.
Target "Zip" (fun _ ->
    !! (buildDir + "/**/*.*")
        -- "*.zip"
        |> Zip buildDir (deployDir + projectName + "." + version + ".zip")
)

Target "Default" (fun _ ->
    trace "Hello World from FAKE"
)

// Dependencies
"Clean"
    ==> "BuildApp"
    //==> "BuildTest"
    //==> "Test"
    ==> "TestParallel"
    ==> "Zip"
    ==> "BuildNuGet"
    ==> "Default"

// start build
RunTargetOrDefault "Default"