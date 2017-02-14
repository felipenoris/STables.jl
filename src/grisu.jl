
using Base.Grisu

function tostring(x::AbstractFloat, n::Int=15)
	#
	# Based on Base.Grisu._show(io::IO, x::AbstractFloat, mode, n::Int, typed, compact)
	# See julia/base/grisu/grisu.jl
	#
	const mode = Base.Grisu.SHORTEST
	#const typed = false
	#const compact = false

    isnan(x) && return "NaN"
    if isinf(x)
    	if signbit(x)
    		return "-Inf"
    	else
    		return "Inf"
    	end
    end

    (len,pt,neg), buffer = Grisu.grisu(x,mode,n), Grisu.DIGITS
    pdigits = pointer(buffer)
    
    #=
    if mode == PRECISION
        while len > 1 && buffer[len] == 0x30
            len -= 1
        end
    end
    =#

    io = IOBuffer()
    if neg
    	write(io, "-")
    end

    #=
    exp_form = pt <= -4 || pt > 6
    exp_form = exp_form || (pt >= len && abs(mod(x + 0.05, 10^(pt - len)) - 0.05) > 0.05) # see issue #6608
    if exp_form # .00001 to 100000.
        # => #.#######e###
        unsafe_write(io, pdigits, 1)
        write(io, '.')
        if len > 1
            unsafe_write(io, pdigits+1, len-1)
        else
            write(io, '0')
        end
        write(io, (typed && isa(x,Float32)) ? 'f' : 'e')
        write(io, dec(pt-1))
        typed && isa(x,Float16) && write(io, ")")
        return
    elseif pt <= 0
    =#
    if pt <= 0
        # => 0.00########
        write(io, "0.")
        while pt < 0
            write(io, '0')
            pt += 1
        end
        unsafe_write(io, pdigits, len)
    elseif pt >= len
        # => ########00.0
        unsafe_write(io, pdigits, len)
        while pt > len
            write(io, '0')
            len += 1
        end
        write(io, ".0")
    else # => ####.####
        unsafe_write(io, pdigits, pt)
        write(io, '.')
        unsafe_write(io, pdigits+pt, len-pt)
    end
    #typed && !compact && isa(x,Float32) && write(io, "f0")
    #typed && isa(x,Float16) && write(io, ")")
    return String(io)
end
