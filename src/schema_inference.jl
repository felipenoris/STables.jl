
function integer_regex(fm::CSVFormat)
    if fm.thousands_separator == nothing
        return r"^\s*-?\d+\s*$"
    else
        if fm.thousands_separator == '.'
            return r"^\s*-?((\d{1,3}\.)?(\d{3}\.)*\d{3}|\d{1,3})\s*$"
        elseif fm.thousands_separator == ','
            return r"^\s*-?((\d{1,3},)?(\d{3},)*\d{3}|\d{1,3})\s*$"
        else
            error("thousands_separator ($(fm.thousands_separator)) not supported.")
        end
    end
end

function float_regex(fm::CSVFormat)
    if fm.thousands_separator == nothing
        if fm.decimal_separator == '.'
            return r"^\s*-?(\d+(\.\d*)?|\d*\.\d+)\s*$"
        elseif fm.decimal_separator == ','
            return r"^\s*-?(\d+(,\d*)?|\d*,\d+)\s*$"
        else
            error("Decimal separator not supported: $(fm.decimal_separator)")
        end
    else
        if fm.thousands_separator == '.' && fm.decimal_separator == ','
            return r"^\s*-?((\d{1,3}\.)?(\d{3}\.)*\d{3}|\d{1,3})(,\d*)?\s*$"
        elseif fm.thousands_separator == ',' && fm.decimal_separator == '.'
            return r"^\s*-?((\d{1,3},)?(\d{3},)*\d{3}|\d{1,3})(\.\d*)?\s*$"
        else
            error("Combination of thousands_separator '$(fm.thousands_separator)' and decimal_separator '$(fm.decimal_separator)' not supported.")
        end
    end
end

mutable struct InferenceState
    datatype::DataType
    allow_missing::Bool
end

InferenceState() = InferenceState(Any, false)

function datatype(s::InferenceState)
    if s.allow_missing
        return Union{Missing, s.datatype}
    else
        return s.datatype
    end
end

Base.:(==)(s1::InferenceState, s2::InferenceState) = s1.datatype == s2.datatype && s1.allow_missing == s2.allow_missing

function infer_type(value::String, format::CSVFormat, state::InferenceState=InferenceState(), fr::Regex=float_regex(format), ir::Regex=integer_regex(format)) :: InferenceState
    inferred_type = Any
    inferred_missing = false

    if ismissingstr(value, format)
        inferred_missing = true
    else
        # If it's already a String, there's nothing to improve with inference
        if state.datatype == String
            inferred_type = String
        else

            if occursin(ir, value)
                inferred_type = Int
            elseif occursin(fr, value)
                inferred_type = Float64
            else
                try
                    Date(value, format.date_format)
                    inferred_type = Date
                catch e
                    inferred_type = String
                end
            end
        end
    end

    # Promote type
    if type_order(inferred_type) > type_order(state)
        state.datatype = inferred_type
    end

    # Promote Missing
    state.allow_missing = state.allow_missing || inferred_missing
    return state
end

type_order(state::InferenceState) = type_order(state.datatype)
type_order(::Type{Any}) = 0
type_order(::Type{Int}) = 1
type_order(::Type{Float64}) = 2
type_order(::Type{Date}) = 3
type_order(::Type{String}) = 4

function infer_schema(raw::Array{String, 2}, format::CSVFormat=CSVFormat(), header::Bool=true) :: Schema
    FST_DATAROW = header ? 2 : 1
    fr = float_regex(format)
    ir = integer_regex(format)
    local s::InferenceState
    (rows, cols) = size(raw)
    @assert rows >= FST_DATAROW "Empty data"

    schema_header = Vector{Symbol}(undef, cols)
    if header
        @inbounds for c in 1:cols
            schema_header[c] = Symbol(raw[1,c])
        end
    else
        @inbounds for c in 1:cols
            schema_header[c] = Symbol(c)
        end
    end

    schema_datatypes = Vector{Type}(undef, cols)

    @inbounds for c in 1:cols
        s = InferenceState()

        for r in FST_DATAROW:rows
            infer_type(raw[r,c], format, s, fr, ir)
        end

        schema_datatypes[c] = datatype(s)
    end

    return Schema(schema_header, schema_datatypes)
end

function infer_schema(input, format::CSVFormat=CSVFormat(); header::Bool=true, use_mmap::Bool=false)
    raw = readdlm(input, format.dlm, String; use_mmap=use_mmap)
    return infer_schema(raw, format, header)
end
