using Test
using ChunkedJSONL
using ChunkedJSONL: ValueExtractionContext
alg=:serial

@testset "Single elements" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "Int $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1"), ctx, _force=alg)

            @test ctx.elements[1] == 1
        end
        @testset "Float64 $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1.0"), ctx, _force=alg)

            @test ctx.elements[1] == 1.0
        end
        @testset "String $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\"1.0\""), ctx, _force=alg)

            @test ctx.elements[1] == "1.0"
        end
        @testset "Bool $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("false"), ctx, _force=alg)
            ChunkedJSONL.parse_file(IOBuffer("true"), ctx, _force=alg)

            @test ctx.elements[1] == false
            @test ctx.elements[2] == true
        end
        @testset "Null $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("null"), ctx, _force=alg)

            @test ctx.elements[1] === nothing
        end
        @testset "Array $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[]"), ctx, _force=alg)

            @test ctx.elements[1] == []

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1, 2]"), ctx, _force=alg)

            @test ctx.elements[1] == [1, 2]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1.0, 2.0]"), ctx, _force=alg)

            @test ctx.elements[1] == [1.0, 2.0]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[\"1\", \"2\"]"), ctx, _force=alg)

            @test ctx.elements[1] == ["1", "2"]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[true, false]"), ctx, _force=alg)

            @test ctx.elements[1] == [true, false]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[null, null]"), ctx, _force=alg)

            @test ctx.elements[1] == [nothing, nothing]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[{}]"), ctx, _force=alg)

            @test ctx.elements[1] == [Dict{Symbol,Any}()]
        end
        @testset "Object $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}()

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => 1)

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1.0}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => 1.0)

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": \"1\"}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => "1")

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": true}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => true)

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": null}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => nothing)

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": []}"), ctx, _force=alg)

            @test ctx.elements[1] == Dict{Symbol,Any}(:a => [])
        end
    end
end


@testset "Multiple lines small buffer" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "Int $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1\n1"), ctx, _force=alg, buffersize=2)

            @test ctx.elements == [1,1]
        end
        @testset "Float64 $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1.0\n1.0"), ctx, _force=alg, buffersize=4)

            @test ctx.elements == [1.0, 1.0]
        end
        @testset "String $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\"1.0\"\n\"1.0\""), ctx, _force=alg, buffersize=6)

            @test ctx.elements == ["1.0","1.0"]
        end
        @testset "Bool $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("false\ntrue"), ctx, _force=alg, buffersize=6)

            @test ctx.elements == [false, true]
        end
        @testset "Null $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("null\nnull"), ctx, _force=alg, buffersize=5)

            @test ctx.elements == [nothing,nothing]
        end
        @testset "Array $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[]\n[]"), ctx, _force=alg, buffersize=3)

            @test ctx.elements == [[],[]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1, 2]\n[1, 2]"), ctx, _force=alg, buffersize=7)

            @test ctx.elements == [[1, 2],[1, 2]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1.0, 2.0]\n[1.0, 2.0]"), ctx, _force=alg, buffersize=11)

            @test ctx.elements == [[1.0, 2.0],[1.0, 2.0]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[\"1\", \"2\"]\n[\"1\", \"2\"]"), ctx, _force=alg, buffersize=11)

            @test ctx.elements == [["1", "2"],["1", "2"]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[true, false]\n[true, false]"), ctx, _force=alg, buffersize=14)

            @test ctx.elements == [[true, false],[true, false]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[null, null]\n[null, null]"), ctx, _force=alg, buffersize=13)

            @test ctx.elements == [[nothing, nothing],[nothing, nothing]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[{}]\n[{}]"), ctx, _force=alg, buffersize=5)

            @test ctx.elements == [[Dict{Symbol,Any}()],[Dict{Symbol,Any}()]]
        end
        @testset "Object $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{}\n{}"), ctx, _force=alg, buffersize=3)

            @test ctx.elements == [Dict{Symbol,Any}(),Dict{Symbol,Any}()]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1}\n{\"a\": 1}"), ctx, _force=alg, buffersize=9)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1),Dict{Symbol,Any}(:a => 1)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1.0}\n{\"a\": 1.0}"), ctx, _force=alg, buffersize=11)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1.0),Dict{Symbol,Any}(:a => 1.0)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": \"1\"}\n{\"a\": \"1\"}"), ctx, _force=alg, buffersize=11)

            @test ctx.elements == [Dict{Symbol,Any}(:a => "1"),Dict{Symbol,Any}(:a => "1")]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": true}\n{\"a\": true}"), ctx, _force=alg, buffersize=12)

            @test ctx.elements == [Dict{Symbol,Any}(:a => true),Dict{Symbol,Any}(:a => true)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": null}\n{\"a\": null}"), ctx, _force=alg, buffersize=12)

            @test ctx.elements == [Dict{Symbol,Any}(:a => nothing),Dict{Symbol,Any}(:a => nothing)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": []}\n{\"a\": []}"), ctx, _force=alg, buffersize=10)

            @test ctx.elements == [Dict{Symbol,Any}(:a => []),Dict{Symbol,Any}(:a => [])]
        end
    end
end

@testset "Multiple lines" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "Int $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1\n1"), ctx, _force=alg)

            @test ctx.elements == [1,1]
        end
        @testset "Float64 $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("1.0\n1.0"), ctx, _force=alg)

            @test ctx.elements == [1.0, 1.0]
        end
        @testset "String $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\"1.0\"\n\"1.0\""), ctx, _force=alg)

            @test ctx.elements == ["1.0","1.0"]
        end
        @testset "Bool $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("false\ntrue"), ctx, _force=alg)

            @test ctx.elements == [false, true]
        end
        @testset "Null $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("null\nnull"), ctx, _force=alg)

            @test ctx.elements == [nothing,nothing]
        end
        @testset "Array $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[]\n[]"), ctx, _force=alg)

            @test ctx.elements == [[],[]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1, 2]\n[1, 2]"), ctx, _force=alg)

            @test ctx.elements == [[1, 2],[1, 2]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[1.0, 2.0]\n[1.0, 2.0]"), ctx, _force=alg)

            @test ctx.elements == [[1.0, 2.0],[1.0, 2.0]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[\"1\", \"2\"]\n[\"1\", \"2\"]"), ctx, _force=alg)

            @test ctx.elements == [["1", "2"],["1", "2"]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[true, false]\n[true, false]"), ctx, _force=alg)

            @test ctx.elements == [[true, false],[true, false]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[null, null]\n[null, null]"), ctx, _force=alg)

            @test ctx.elements == [[nothing, nothing],[nothing, nothing]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("[{}]\n[{}]"), ctx, _force=alg)

            @test ctx.elements == [[Dict{Symbol,Any}()],[Dict{Symbol,Any}()]]
        end
        @testset "Object $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{}\n{}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(),Dict{Symbol,Any}()]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1}\n{\"a\": 1}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1),Dict{Symbol,Any}(:a => 1)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": 1.0}\n{\"a\": 1.0}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1.0),Dict{Symbol,Any}(:a => 1.0)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": \"1\"}\n{\"a\": \"1\"}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => "1"),Dict{Symbol,Any}(:a => "1")]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": true}\n{\"a\": true}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => true),Dict{Symbol,Any}(:a => true)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": null}\n{\"a\": null}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => nothing),Dict{Symbol,Any}(:a => nothing)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("{\"a\": []}\n{\"a\": []}"), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => []),Dict{Symbol,Any}(:a => [])]
        end
    end
end

@testset "Multiple lines leading and trailing whitespace" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "Int $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" 1 \n 1 "), ctx, _force=alg)

            @test ctx.elements == [1,1]
        end
        @testset "Float64 $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" 1.0 \n 1.0 "), ctx, _force=alg)

            @test ctx.elements == [1.0, 1.0]
        end
        @testset "String $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" \"1.0\" \n \"1.0\" "), ctx, _force=alg)

            @test ctx.elements == ["1.0","1.0"]
        end
        @testset "Bool $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" false \n true "), ctx, _force=alg)

            @test ctx.elements == [false, true]
        end
        @testset "Null $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" null \n null "), ctx, _force=alg)

            @test ctx.elements == [nothing,nothing]
        end
        @testset "Array $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [] \n [] "), ctx, _force=alg)

            @test ctx.elements == [[],[]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [1, 2] \n [1, 2] "), ctx, _force=alg)

            @test ctx.elements == [[1, 2],[1, 2]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [1.0, 2.0] \n [1.0, 2.0] "), ctx, _force=alg)

            @test ctx.elements == [[1.0, 2.0],[1.0, 2.0]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [\"1\", \"2\"] \n [\"1\", \"2\"] "), ctx, _force=alg)

            @test ctx.elements == [["1", "2"],["1", "2"]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [true, false] \n [true, false] "), ctx, _force=alg)

            @test ctx.elements == [[true, false],[true, false]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [null, null] \n [null, null] "), ctx, _force=alg)

            @test ctx.elements == [[nothing, nothing],[nothing, nothing]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" [{}] \n [{}] "), ctx, _force=alg)

            @test ctx.elements == [[Dict{Symbol,Any}()],[Dict{Symbol,Any}()]]
        end
        @testset "Object $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {} \n {} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(),Dict{Symbol,Any}()]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": 1} \n {\"a\": 1} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1),Dict{Symbol,Any}(:a => 1)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": 1.0} \n {\"a\": 1.0} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1.0),Dict{Symbol,Any}(:a => 1.0)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": \"1\"} \n {\"a\": \"1\"} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => "1"),Dict{Symbol,Any}(:a => "1")]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": true} \n {\"a\": true} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => true),Dict{Symbol,Any}(:a => true)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": null} \n {\"a\": null} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => nothing),Dict{Symbol,Any}(:a => nothing)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer(" {\"a\": []} \n {\"a\": []} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => []),Dict{Symbol,Any}(:a => [])]
        end
    end
end


@testset "Multiple lines leading and trailing whitespace with BOM" begin
    for alg in [:serial, :singlebuffer, :doublebuffer]
        @testset "Int $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf 1 \n 1 "), ctx, _force=alg)

            @test ctx.elements == [1,1]
        end
        @testset "Float64 $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf 1.0 \n 1.0 "), ctx, _force=alg)

            @test ctx.elements == [1.0, 1.0]
        end
        @testset "String $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf \"1.0\" \n \"1.0\" "), ctx, _force=alg)

            @test ctx.elements == ["1.0","1.0"]
        end
        @testset "Bool $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf false \n true "), ctx, _force=alg)

            @test ctx.elements == [false, true]
        end
        @testset "Null $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf null \n null "), ctx, _force=alg)

            @test ctx.elements == [nothing,nothing]
        end
        @testset "Array $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [] \n [] "), ctx, _force=alg)

            @test ctx.elements == [[],[]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [1, 2] \n [1, 2] "), ctx, _force=alg)

            @test ctx.elements == [[1, 2],[1, 2]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [1.0, 2.0] \n [1.0, 2.0] "), ctx, _force=alg)

            @test ctx.elements == [[1.0, 2.0],[1.0, 2.0]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [\"1\", \"2\"] \n [\"1\", \"2\"] "), ctx, _force=alg)

            @test ctx.elements == [["1", "2"],["1", "2"]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [true, false] \n [true, false] "), ctx, _force=alg)

            @test ctx.elements == [[true, false],[true, false]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [null, null] \n [null, null] "), ctx, _force=alg)

            @test ctx.elements == [[nothing, nothing],[nothing, nothing]]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf [{}] \n [{}] "), ctx, _force=alg)

            @test ctx.elements == [[Dict{Symbol,Any}()],[Dict{Symbol,Any}()]]
        end
        @testset "Object $alg" begin
            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {} \n {} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(),Dict{Symbol,Any}()]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": 1} \n {\"a\": 1} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1),Dict{Symbol,Any}(:a => 1)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": 1.0} \n {\"a\": 1.0} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => 1.0),Dict{Symbol,Any}(:a => 1.0)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": \"1\"} \n {\"a\": \"1\"} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => "1"),Dict{Symbol,Any}(:a => "1")]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": true} \n {\"a\": true} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => true),Dict{Symbol,Any}(:a => true)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": null} \n {\"a\": null} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => nothing),Dict{Symbol,Any}(:a => nothing)]

            ctx = ValueExtractionContext()
            ChunkedJSONL.parse_file(IOBuffer("\xef\xbb\xbf {\"a\": []} \n {\"a\": []} "), ctx, _force=alg)

            @test ctx.elements == [Dict{Symbol,Any}(:a => []),Dict{Symbol,Any}(:a => [])]
        end
    end
end
