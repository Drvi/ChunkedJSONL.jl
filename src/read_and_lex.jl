mutable struct LexerState{IO_t<:IO}
    io::IO_t
    last_newline_at::UInt32
    done::Bool

    LexerState(input::IO) = new{typeof(input)}(input, UInt32(0), false)
end


function hasBOM(bytes::Vector{UInt8})
    return @inbounds bytes[1] == 0xef && bytes[2] == 0xbb && bytes[3] == 0xbf
end

end_of_stream(io::IO) = eof(io)
end_of_stream(io::GzipDecompressorStream) = eof(io)

readbytesall!(io::IOStream, buf, n) = UInt32(Base.readbytes!(io, buf, n; all = true))
readbytesall!(io::IO, buf, n) = UInt32(Base.readbytes!(io, buf, n))

function _skip_over_initial_whitespace_and_bom!(io, buf, bytes_read_in)
    bytes_read_in == 0 && return bytes_read_in # empty input -- nothing to skip
    buffersize = UInt32(length(buf))
    starts_with_bom = bytes_read_in > 2 && hasBOM(buf)
    first_byte_to_check = (starts_with_bom ? 4 : 1)
    # First byte that is not a whitespace or a BOM, zero if the entire buffer is only white
    # space
    populated = view(buf, 1:bytes_read_in)
    first_valid_byte = something(findnext(_nonspace, populated, first_byte_to_check), 0)

    # We'll keep refilling the buffer until it we find a non-space or we run out of bytes
    while first_valid_byte == 0
        bytes_read_in = readbytesall!(io, buf, buffersize)
        bytes_read_in == 0 && return bytes_read_in
        populated = view(buf, 1:bytes_read_in)
        first_valid_byte = something(findnext(_nonspace, populated, 1), 0)
    end

    # We found a non-space byte -- we'll left-shift the spaces before it so that the buffer
    # begins with a valid value and we'll try to refill the rest of the buffer.
    # If first_valid_byte was already at the beginnig of the buffer, we don't have to do
    # anything.
    skip_over = UInt32(first_valid_byte - 1)
    if skip_over > 0
        bytes_read_in += prepare_buffer!(io, buf, skip_over)
    end
    return bytes_read_in - skip_over
end

function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_newline_at::UInt32)
    ptr = pointer(buf)
    buffersize = UInt32(length(buf))
    if last_newline_at == 0
        # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = readbytesall!(io, buf, buffersize)
        bytes_read_in = _skip_over_initial_whitespace_and_bom!(io, buf, bytes_read_in)
    elseif last_newline_at < buffersize
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_newline_at, buffersize - last_newline_at)
        bytes_read_in = @inbounds readbytesall!(io, @view(buf[buffersize - last_newline_at + 1:end]), last_newline_at)
    else
        # Last chunk was consumed entirely
        bytes_read_in = readbytesall!(io, buf, buffersize)
    end
    return bytes_read_in
end

# We process input data iteratively by populating a buffer from IO.
# In each iteration we first lex the newlines and then parse them in parallel.
findmark(ptr, bytes_to_search, b::UInt8) = something(memchr(ptr, bytes_to_search, b), zero(UInt))
function read_and_lex!(lexer_state::LexerState, parsing_ctx::ParsingContext)
    ptr = pointer(parsing_ctx.bytes) # We never resize the buffer, the array shouldn't need to relocate
    buf = parsing_ctx.bytes

    empty!(parsing_ctx.eols)
    push!(parsing_ctx.eols, UInt32(0))
    eols = parsing_ctx.eols
    buffersize = UInt32(length(buf))

    bytes_read_in = prepare_buffer!(lexer_state.io, parsing_ctx.bytes, lexer_state.last_newline_at)
    reached_end_of_file = end_of_stream(lexer_state.io)

    bytes_to_search = UInt(bytes_read_in)
    if lexer_state.last_newline_at == UInt(0) # first time populating the buffer
        bytes_carried_over_from_previous_chunk = UInt32(0)
        offset = bytes_carried_over_from_previous_chunk
    else
        bytes_carried_over_from_previous_chunk = buffersize - lexer_state.last_newline_at
        offset = bytes_carried_over_from_previous_chunk
        ptr += offset
    end
    @inbounds while bytes_to_search > UInt(0)
        pos_to_check = findmark(ptr, bytes_to_search, UInt8('\n'))
        offset += UInt32(pos_to_check)
        if pos_to_check == UInt(0)
            if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !reached_end_of_file
                close(lexer_state.io)
                throw(NoValidRowsInBufferError(UInt32(length(buf))))
            end
            break
        else
            push!(eols, offset)
            ptr += pos_to_check
            bytes_to_search -= pos_to_check
        end
    end

    @inbounds if reached_end_of_file
        lexer_state.done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last_byte = bytes_carried_over_from_previous_chunk + bytes_read_in
        last(eols) < last_byte && push!(eols, last_byte + UInt32(1))
    else
        lexer_state.done = false
    end
    lexer_state.last_newline_at = last(eols)
    return nothing
end

mutable struct MmapStream{IO_t<:IO} <: IO
    ios::IO_t
    x::Vector{UInt8}
    pos::Int
end
MmapStream(ios::IO) = MmapStream(ios, mmap(ios, grow=false, shared=false), 1)
Base.close(m::MmapStream) = close(m.ios)
Base.eof(m::MmapStream) = m.pos == length(m.x)
function readbytesall!(io::MmapStream, buf, n)
    bytes_to_read = min(bytesavailable(io), n)
    unsafe_copyto!(pointer(buf), pointer(io.x) + io.pos - 1, bytes_to_read)
    io.pos += bytes_to_read
    return UInt32(bytes_to_read)
end
# Interop with GzipDecompressorStream
Base.bytesavailable(m::MmapStream) = length(m.x) - m.pos
Base.isopen(m::MmapStream) = isopen(m.ios) && !eof(m)
function Base.unsafe_read(from::MmapStream, p::Ptr{UInt8}, nb::UInt)
    avail = bytesavailable(from)
    adv = min(avail, nb)
    GC.@preserve from unsafe_copyto!(p, pointer(from.x) + from.pos - 1, adv)
    from.pos += adv
    if nb > avail
        throw(EOFError())
    end
    nothing
end

_input_to_io(input::IO, use_mmap::Bool) = false, input
function _input_to_io(input::String, use_mmap::Bool)
    ios = open(input, "r")
    if !eof(ios) && peek(ios, UInt16) == 0x8b1f
        # TODO: GzipDecompressorStream doesn't respect MmapStream reaching EOF for some reason
        # io = CodecZlib.GzipDecompressorStream(use_mmap ? MmapStream(ios) : ios, stop_on_end=use_mmap)
        use_mmap && @warn "`use_mmap=true` is currently unsupported when reading gzipped files, using file io."
        io = CodecZlib.GzipDecompressorStream(ios)
        TranscodingStreams.changemode!(io, :read)
    elseif use_mmap
        io = MmapStream(ios)
    else
        io = ios
    end
    return true, io
end
