
function non_float_regex(fm::CSVFormat)
    if isnull(fm.thousands_separator)
        if fm.decimal_separator == '.'
            return r"[^\d\.-]"
        elseif fm.decimal_separator == ','
            return r"[^\d,-]"
        else
            error("Decimal separator not supported: $(fm.decimal_separator)")
        end
    else
        ts = get(fm.thousands_separator)
        if (ts == '.' && fm.decimal_separator == ',') || (ts == ',' && fm.decimal_separator == '.')
            return r"[^\d\.,-]"
        else
            error("Combination of thousands_separator ($ts) and decimal_separator $(fm.decimal_separator) not supported.")
        end
    end
end

function non_integer_regex(fm::CSVFormat)
    if isnull(fm.thousands_separator)
        return r"[^\d-]"
    else
        ts = get(fm.thousands_separator)
        if ts == '.'
            return r"[^\d-\.]"
        elseif ts == ','
            return r"[^\d-,]"
        else
            error("thousands_separator ($ts) not supported.")
        end
    end
end

type InferenceState
	datatype::DataType
	isnullable::Bool
end

InferenceState() = InferenceState(Any, false)

function datatype(s::InferenceState)
	if s.isnullable
		return Nullable{s.datatype}
	else
		return s.datatype
	end
end

Base.:(==)(s1::InferenceState, s2::InferenceState) = s1.datatype == s2.datatype && s1.isnullable == s2.isnullable

function infer_type(value::String, format::CSVFormat, state::InferenceState=InferenceState(), nfr::Regex=non_float_regex(format), nir::Regex=non_integer_regex(format)) :: InferenceState
	inferred_type = Any
	inferred_nullable = false

	if isnullstr(value, format)
		inferred_nullable = true
	else
		# If it's already a String, there's nothing to improve with inference
		if state.datatype == String
			inferred_type = String
		else
			if ismatch(nfr, value)
				try 
					Date(value, format.date_format)
					inferred_type = Date
				catch e
					inferred_type = String
				end
			elseif ismatch(nir, value)
				inferred_type = Float64
			else
				inferred_type = Int
			end
		end
	end

	# Promote type
	if type_order(inferred_type) > type_order(state)
		state.datatype = inferred_type
	end

	# Promote Nullable
	state.isnullable = state.isnullable || inferred_nullable
	return state
end

type_order(state::InferenceState) = type_order(state.datatype)
type_order(::Type{Any}) = 0
type_order(::Type{Int}) = 1
type_order(::Type{Float64}) = 2
type_order(::Type{Date}) = 3
type_order(::Type{String}) = 4

function infer_schema(raw::Array{String, 2}, format::CSVFormat, header::Bool=true) :: Schema
	const FST_DATAROW = header ? 2 : 1
	const nfr = non_float_regex(format)
	const nir = non_integer_regex(format)
	local s::InferenceState
	(rows, cols) = size(raw)
	@assert rows >= FST_DATAROW "Empty data"

	schema_header = Vector{Symbol}(cols)
	if header
		for c in 1:cols
			schema_header[c] = Symbol(raw[1,c])
		end
	else
		for c in 1:cols
			schema_header[c] = Symbol(c)
		end
	end

	schema_datatypes = Vector{DataType}(cols)

	for c in 1:cols
		s = InferenceState()
		for r in FST_DATAROW:rows
			infer_type(raw[r,c], format, s, nfr, nir)
		end
		schema_datatypes[c] = datatype(s)
	end

	return Schema(schema_header, schema_datatypes)
end
