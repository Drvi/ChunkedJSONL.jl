using Test

Threads.nthreads() == 1 && @warn "Running tests with a single thread -- won't be able to spot concurrency issues"

@testset "ChunkedJSONL.jl" begin
    include("lexer_tests.jl")
    include("basic_tests.jl")
    include("exception_handling.jl")
end
