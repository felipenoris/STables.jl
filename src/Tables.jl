
__precompile__(true)
module Tables

@assert VERSION >= v"0.6.0" "Tables.jl requires Julia v0.6.0 or newer."

using NullableArrays, Missings, DataFrames
using Lifting

include("grisu.jl")
include("types.jl")
include("schema.jl")
include("table.jl")
include("tablerow.jl")
include("schema_inference.jl")
include("csv.jl")

export Table, Schema

end # module Tables
