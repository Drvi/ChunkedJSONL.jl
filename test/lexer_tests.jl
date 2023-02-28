using ChunkedJSONL: prepare_buffer!
using Test

@testset "initial buffer fill" begin
    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer(""), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer(" "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer(" "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer("  "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 2);
    @test prepare_buffer!(IOBuffer("  "), buf, UInt32(0)) == 0


    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer(" 1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer(" 1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer("  1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 2);
    @test prepare_buffer!(IOBuffer("  1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer("1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 2);
    @test prepare_buffer!(IOBuffer("1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')


    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("12"), buf, UInt32(0)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')

    buf = zeros(UInt8, 1);
    @test prepare_buffer!(IOBuffer("12"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 2);
    @test prepare_buffer!(IOBuffer("12"), buf, UInt32(0)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')
end

@testset "initial buffer fill with BOM" begin
    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf"), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 4);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 5);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf  "), buf, UInt32(0)) == 0

    buf = zeros(UInt8, 5);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf  "), buf, UInt32(0)) == 0


    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf 1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 4);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf 1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 4);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf  1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 5);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf  1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 4);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')

    buf = zeros(UInt8, 5);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf1"), buf, UInt32(0)) == 1
    @test buf[1] == UInt8('1')


    buf = zeros(UInt8, 10);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf12"), buf, UInt32(0)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')

    buf = zeros(UInt8, 4);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf12"), buf, UInt32(0)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')

    buf = zeros(UInt8, 5);
    @test prepare_buffer!(IOBuffer("\xef\xbb\xbf12"), buf, UInt32(0)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')
end

@testset "buffer refill" begin
    buf = zeros(UInt8, 10)
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test prepare_buffer!(io, buf, UInt32(10)) == 2
    @test buf[1] == UInt8('1')
    @test buf[2] == UInt8('2')

    buf = zeros(UInt8, 10)
    buf[9] = 0x09
    buf[10] = 0x0a
    io = IOBuffer("xxx12")
    skip(io, 3)
    @test prepare_buffer!(io, buf, UInt32(8)) == 2
    @test buf[1] == 0x09
    @test buf[2] == 0x0a
    @test buf[3] == UInt8('1')
    @test buf[4] == UInt8('2')
end
