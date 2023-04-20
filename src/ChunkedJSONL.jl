module ChunkedJSONL

export parse_file, consume!, DebugContext, AbstractConsumeContext

using ScanByte
import JSON3
using .Threads: @spawn
using TranscodingStreams
using CodecZlib
using Mmap

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

include("BufferedVectors.jl")
include("TaskResults.jl")

include("exceptions.jl")

mutable struct TaskCondition
    ntasks::Int
    cond_wait::Threads.Condition
    exception::Union{Nothing, Exception}
end
TaskCondition() = TaskCondition(0, Threads.Condition(ReentrantLock()), nothing)

struct ParsingContext
    bytes::Vector{UInt8}
    eols::BufferedVector{UInt32}
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
    cond::TaskCondition
end
function estimate_task_size(parsing_ctx::ParsingContext)
    length(parsing_ctx.eols) == 1 && return 1 # empty file
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, ceil(Int, last(parsing_ctx.eols)  / length(parsing_ctx.eols))))
    return max(min_rows, cld(ceil(Int, length(parsing_ctx.eols) * ((1 + length(parsing_ctx.bytes)) / (1 + last(parsing_ctx.eols)))), parsing_ctx.maxtasks))
end
_nonspace(b::UInt8) = !isspace(Char(b))

include("read_and_lex.jl")
# include("init_parsing.jl")
include("consume_context.jl")

include("row_parsing.jl")
include("parser_serial.jl")
include("parser_singlebuffer.jl")
include("parser_doublebuffer.jl")


function parse_file(
    input,
    consume_ctx::AbstractConsumeContext=DebugContext();
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2nworkers,
    nresults::Integer=maxtasks,
    use_mmap::Bool=false,
    _force::Symbol=:none,
)
    0 < buffersize < typemax(UInt32) || throw(ArgumentError("`buffersize` argument must be larger than 0 and smaller than 4_294_967_295 bytes."))
    nworkers > 0 || throw(ArgumentError("`nworkers` argument must be larger than 0."))
    maxtasks >= nworkers || throw(ArgumentError("`maxtasks` argument must be larger of equal to the `nworkers` argument."))
    # otherwise not implemented; implementation postponed once we know why take!/wait allocates
    nresults == maxtasks || throw(ArgumentError("Currently, `nresults` argument must be equal to the `maxtasks` argument."))

    should_close, io = _input_to_io(input, use_mmap)

    lexer_state = LexerState(io)
    parsing_ctx = ParsingContext(Vector{UInt8}(undef, buffersize), BufferedVector{UInt32}(), nworkers, maxtasks, nresults, TaskCondition())
    read_and_lex!(lexer_state, parsing_ctx)

    if _force === :doublebuffer
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx)
    elseif _force === :singlebuffer
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx)
    elseif _force === :serial || Threads.nthreads() == 1 || parsing_ctx.nworkers == 1 || parsing_ctx.maxtasks == 1 || lexer_state.last_newline_at < MIN_TASK_SIZE_IN_BYTES
              _parse_file_serial(lexer_state, parsing_ctx, consume_ctx)
    elseif !lexer_state.done
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx)
    else
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx)
    end
    # end
    should_close && close(io)
    return nothing
end


end # module
