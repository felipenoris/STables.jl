
function Base.:(==)(sa::Schema, sb::Schema)
    return (sa.names == sb.names) && (sa.types == sb.types)
end

# shallow-copy
function Base.copy(s::Schema)
    new_header = copy(s.names)
    new_types = copy(s.types)
    return Schema(new_header, new_types)
end

function Base.deepcopy(s::Schema)
    new_header = deepcopy(s.names)
    new_types = deepcopy(s.types)
    return Schema(new_header, new_types)
end

function Schema(header::Vector, types::Vector{DataType})
    header = [Symbol(x) for x in header]
    return Schema(header, types)
end

# Allow for syntax: Schema( [:a => String, :b => Int] )
function Schema(v::Vector{Pair{Symbol, DataType}})
    n = length(v)
    head = Vector{Symbol}(n)
    types = Vector{DataType}(n)

    for i in 1:n
        head[i] = v[i][1]
        types[i] = v[i][2]
    end

    return Schema(head, types)
end

# Returns [ :a => String, :b => Int ]
function pairs(s::Schema)
    n = ncol(s)
    result = Vector{Pair{Symbol, DataType}}(n)

    for i in 1:n
        result[i] = s.names[i] => s.types[i]
    end
    return result
end

# Allow syntax: Schema(a=String, b=Int)
function Schema(; kwargs...)
    n = length(kwargs)
    head = Vector{Symbol}(n)
    types = Vector{DataType}(n)
    for i in 1:n
        k, v = kwargs[i]
        head[i] = k
        types[i] = isa(v, DataType) ? v : eltype(v)
    end

    return Schema(head, types)
end

function Base.push!(s::Schema, column_description::Pair{Symbol, DataType})
    push!(s.names, column_description[1])
    push!(s.types, column_description[2])
end

DataFrames.names(s::Schema) = s.names

function DataFrames.names!(s::Schema, new_names::Vector{Symbol})
    s.names = new_names
end

DataFrames.ncol(s::Schema) = length(s.names)
