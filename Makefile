.PHONY: debug release clean format

debug:
	cmake -B build \
	      -DCMAKE_BUILD_TYPE=Debug
	cmake --build build --parallel

release:
	cmake -B build \
	      -DCMAKE_BUILD_TYPE=RelWithDebInfo
	cmake --build build --parallel

clean:
	rm -rf build

format:
	find src -name "*.h" -o -name "*.cc" | xargs clang-format -i