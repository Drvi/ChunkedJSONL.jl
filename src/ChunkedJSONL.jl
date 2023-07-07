module ChunkedJSONL

export parse_file, DebugContext, AbstractConsumeContext
export consume!, setup_tasks!, task_done!

using JSON3
using SnoopPrecompile
using ChunkedBase
using SentinelArrays.BufferedVectors

_nonspace(b::UInt8) = !isspace(Char(b))

include("result_buffer.jl")
include("consume_context.jl")

struct ParsingContext <: AbstractParsingContext
    ignoreemptyrows::Bool
    result_buffers::Vector{TaskResultBuffer}
end

include("row_parsing.jl")

function parse_file(
    input,
    consume_ctx::AbstractConsumeContext=DebugContext();
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=Threads.nthreads() * 1024 * 1024,
    nworkers::Integer=Threads.nthreads(),
    limit::Int=0,
    skipto::Int=0,
    comment::Union{Nothing,String,Char,UInt8,Vector{UInt8}}=nothing,
    ignoreemptyrows::Bool=true,
    newlinechar::Union{UInt8,Char,Nothing}=UInt8('\n'),
    use_mmap::Bool=false,
    _force::Symbol=:default,
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    if !isnothing(newlinechar)
        newlinechar = UInt8(newlinechar)
        sizeof(newlinechar) > 1 && throw(ArgumentError("`newlinechar` must be a single-byte character."))
    end

    should_close, io = ChunkedBase._input_to_io(input, use_mmap)
    chunking_ctx = ChunkingContext(buffersize, nworkers, limit, comment)

    # chunking_ctx.bytes is now filled with `bytes_read_in` bytes, we've skipped over BOM
    # and since the third argument is true, we also skipped over any leading whitespace.
    bytes_read_in = ChunkedBase.initial_read!(io, chunking_ctx, true)
    newline = isnothing(newlinechar) ?
        ChunkedBase._detect_newline(chunking_ctx.bytes, 1, bytes_read_in) :
        UInt8(newlinechar)

    lexer = Lexer(io, nothing, newline)
    ChunkedBase.initial_lex!(lexer, chunking_ctx, bytes_read_in)
    ChunkedBase.skip_rows_init!(lexer, chunking_ctx, skipto)

    nrows = length(chunking_ctx.newline_positions) - 1
    if ChunkedBase.should_use_parallel(chunking_ctx, _force)
        ntasks = tasks_per_chunk(chunking_ctx)
        nbuffers = total_result_buffers_count(chunking_ctx)
        result_buffers = TaskResultBuffer[TaskResultBuffer(id, cld(nrows, ntasks)) for id in 1:nbuffers]
    else
        result_buffers = TaskResultBuffer[TaskResultBuffer(0, nrows)]
    end

    parsing_ctx = ParsingContext(ignoreemptyrows, result_buffers)
    try
        if ChunkedBase.should_use_parallel(chunking_ctx, _force)
            parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx)
        else
            parse_file_serial(lexer, parsing_ctx, consume_ctx, chunking_ctx)
        end
    finally
        should_close && close(io)
    end
    return nothing
end


end # module
