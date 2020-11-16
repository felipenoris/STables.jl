
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

function Schema(header::Vector, types::Vector{T}) where {T<:Type}
    header = [Symbol(x) for x in header]
    types = Type[x for x in types]
    return Schema(header, types)
end

# Allow for syntax: Schema( [:a => String, :b => Int] )
function Schema(v::Vector{Pair{Symbol, T}}) where {T<:Type}
    n = length(v)
    head = Vector{Symbol}(undef, n)
    types = Vector{Type}(undef, n)

    @inbounds for i in 1:n
        head[i] = v[i][1]
        types[i] = v[i][2]
    end

    return Schema(head, types)
end

# Returns [ :a => String, :b => Int ]
function pairs(s::Schema)
    n = ncol(s)
    result = Vector{Pair{Symbol, Type}}(undef, n)

    @inbounds for i in 1:n
        result[i] = s.names[i] => s.types[i]
    end
    return result
end

# Allow syntax: Schema(a=String, b=Int)
function Schema(; kwargs...)
    n = length(kwargs)
    head = Vector{Symbol}(undef, n)
    types = Vector{Type}(undef, n)

    @inbounds for (i, p) in enumerate(kwargs)
        k, v = p
        head[i] = k
        types[i] = isa(v, Type) ? v : eltype(v)
    end

    return Schema(head, types)
end

function Base.push!(s::Schema, column_description::Pair{Symbol, T}) where {T<:Type}
    push!(s.names, column_description[1])
    push!(s.types, column_description[2])
end

Base.names(s::Schema) = copy(s.names)

function DataFrames.rename!(s::Schema, new_names::Vector{Symbol})
    s.names = new_names
end

DataFrames.ncol(s::Schema) = length(s.names)
