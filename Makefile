.PHONY: debug release test format clean

debug:
	cmake -B build \
	      -DCMAKE_BUILD_TYPE=Debug
	cmake --build build --parallel

release:
	cmake -B build \
	      -DCMAKE_BUILD_TYPE=RelWithDebInfo
	cmake --build build --parallel

test:
	tests/.venv/bin/python -m pytest -q tests/functional

format:
	find src -name "*.h" -o -name "*.cc" | xargs clang-format -i

clean:
	rm -rf build