using ChunkedJSONL
using ChunkedJSONL: ParsingContext, TaskResultBuffer
using Test


# Throws when a specified row is greater than the first row of a task buffer
struct TestThrowingContext <: AbstractConsumeContext 
    tasks::Vector{Task}
    conds::Vector{ChunkedJSONL.TaskCondition}
    throw_row::Int
end
TestThrowingContext(throw_row) = TestThrowingContext(Task[], ChunkedJSONL.TaskCondition[], throw_row)

# Throws in the last quarter of the buffer
struct ThrowingIO <: IO
    io::IOBuffer
    throw_byte::Int
end
ThrowingIO(s::String) = ThrowingIO(IOBuffer(s), length(s) - cld(length(s), 4))
Base.read(io::ThrowingIO, ::Type{UInt8}) = io.io.ptr > io.throw_byte ? error("That should be enough data for everyone") : read(io.io, UInt8)
ChunkedJSONL.readbytesall!(io::ThrowingIO, buf, n::Int) = io.io.ptr > io.throw_byte ? error("That should be enough data for everyone") : ChunkedJSONL.readbytesall!(io.io, buf, n)
Base.eof(io::ThrowingIO) = Base.eof(io.io)

function ChunkedJSONL.consume!(ctx::TestThrowingContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::UInt32, eol_idx::UInt32)
    t = current_task()
    c = parsing_ctx.cond
    c in ctx.conds || push!(ctx.conds, c)
    t in ctx.tasks || push!(ctx.tasks, t)
    row_num >= ctx.throw_row && error("These contexts are for throwing, and that's all what they do")
    sleep(0.01) # trying to get the task off a fast path to claim everything from the parsing queue
    return nothing
end

@testset "Exception handling" begin
@testset "consume!" begin
    @testset "serial" begin
        throw_ctx = TestThrowingContext(2)
        @test_throws ErrorException("These contexts are for throwing, and that's all what they do") parse_file(IOBuffer("""
            [1,2]
            [3,4]
            """),
            throw_ctx,
            _force=:serial,
            buffersize=6
        )
        @assert !isempty(throw_ctx.tasks)
        @test throw_ctx.tasks[1] === current_task()
        @test throw_ctx.conds[1].exception isa ErrorException
    end

    @testset "singlebuffer" begin
        # 1500 rows should be enough to get each of the 3 task at least one consume!
        throw_ctx = TestThrowingContext(1500) 
        @test_throws TaskFailedException parse_file(
            IOBuffer(("[1,2]\n[3,4]\n" ^ 800)), # 1600 rows total
            throw_ctx,
            nworkers=min(3, Threads.nthreads()),
            _force=:singlebuffer,
            buffersize=12,
        )
        sleep(0.2)
        @test length(throw_ctx.tasks) == min(3, Threads.nthreads())
        @test all(istaskdone, throw_ctx.tasks)
        @test throw_ctx.conds[1].exception isa CapturedException
        @test throw_ctx.conds[1].exception.ex.msg == "These contexts are for throwing, and that's all what they do"
    end

    @testset "doublebuffer" begin
        # 1500 rows should be enough to get each of the 3 task at least one consume!
        throw_ctx = TestThrowingContext(1500) 
        @test_throws TaskFailedException parse_file(
            IOBuffer(("[1,2]\n[3,4]\n" ^ 800)), # 1600 rows total
            throw_ctx,
            nworkers=min(3, Threads.nthreads()),
            _force=:doublebuffer,
            buffersize=12,
        )
        sleep(0.2)
        @test length(throw_ctx.tasks) == min(3, Threads.nthreads())
        @test all(istaskdone, throw_ctx.tasks)
        @test throw_ctx.conds[1].exception isa CapturedException
        @test throw_ctx.conds[1].exception.ex.msg == "These contexts are for throwing, and that's all what they do"
    end
end

@testset "io" begin
    @testset "serial" begin
        throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
        @test_throws ErrorException("That should be enough data for everyone") parse_file(
            ThrowingIO(("[1,2]\n[3,4]\n" ^ 10)), # 20 rows total
            throw_ctx,
            _force=:serial,
            buffersize=6,
        )
        @assert !isempty(throw_ctx.tasks)
        @test throw_ctx.tasks[1] === current_task()
        @test throw_ctx.conds[1].exception isa ErrorException
    end

    @testset "singlebuffer" begin
        throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
        @test_throws TaskFailedException parse_file(
            ThrowingIO(("[1,2]\n[3,4]\n" ^ 800)), # 1600 rows total
            throw_ctx,
            nworkers=min(3, Threads.nthreads()),
            _force=:singlebuffer,
            buffersize=12,
        )
        sleep(0.2)
        @test length(throw_ctx.tasks) == min(3, Threads.nthreads())
        @test all(istaskdone, throw_ctx.tasks)
        @test throw_ctx.conds[1].exception isa CapturedException
        @test throw_ctx.conds[1].exception.ex.task.result.msg == "That should be enough data for everyone"
    end

    @testset "doublebuffer" begin
        throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
        @test_throws TaskFailedException parse_file(
            ThrowingIO(("[1,2]\n[3,4]\n" ^ 800)), # 1600 rows total
            throw_ctx,
            nworkers=min(3, Threads.nthreads()),
            _force=:doublebuffer,
            buffersize=12,
        )
        sleep(0.2)
        @test length(throw_ctx.tasks) == min(3, Threads.nthreads())
        @test all(istaskdone, throw_ctx.tasks)
        @test throw_ctx.conds[1].exception isa CapturedException
        @test throw_ctx.conds[1].exception.ex.task.result.msg == "That should be enough data for everyone"
        @test throw_ctx.conds[2].exception isa CapturedException
        @test throw_ctx.conds[2].exception.ex.task.result.msg == "That should be enough data for everyone"
    end
end
end # @testset "Exception handling"