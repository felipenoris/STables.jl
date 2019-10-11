
module STables

using DataFrames
using Dates
import DelimitedFiles

include("grisu.jl")
include("types.jl")
include("schema.jl")
include("table.jl")
include("tablerow.jl")
include("schema_inference.jl")
include("csv.jl")

end # module STables
