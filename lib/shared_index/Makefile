#### PROJECT SETTINGS ####
# Compiler used
CXX ?= g++
# Extension of source files used in the project
SRC_EXT = cpp
# Path to the source directory, relative to the makefile
SRC_PATH = src
# General compiler flags
COMPILE_FLAGS = -std=c++17 -march=haswell -D__COMPILE_AS_SHARED \
	-Wall -Wextra -Werror -Wno-unused-parameter \
	-fdata-sections -ffunction-sections -fPIC -fvisibility=hidden
# Additional release-specific flags
RCOMPILE_FLAGS = -DNDEBUG -O3
# Additional debug-specific flags
DCOMPILE_FLAGS = -DDEBUG -g
# Add additional include paths
BOOST_HEADERS = $(shell find deps/boost/libs -name "include" | \
					  	xargs -I{} echo "-I./{}")
INCLUDES = -I$(SRC_PATH) -I./include \
	-I./deps/abseil-cpp \
	$(BOOST_HEADERS) \
	-I./deps/fmt/include \
	-I./deps/GSL/include \
	-I./deps/xxHash \
	-I./deps/tkrzw
# General linker settings
ABSL_LIBRARIES = $(shell find deps/out/lib/libabsl_*.a -printf '%f\n' \
		| sed -e 's/libabsl_\([a-z0-9_]\+\)\.a/-labsl_\1/g')
LINK_FLAGS = -shared -Ldeps/out/lib \
	-Wl,-Bstatic -lrt -ltkrzw \
	-Wl,--start-group $(ABSL_LIBRARIES) -Wl,--end-group \
	-lzstd -ljemalloc \
	-Wl,-Bdynamic -lpthread -ldl \
	-Wl,--gc-sections
# Additional release-specific linker settings
RLINK_FLAGS =
# Additional debug-specific linker settings
DLINK_FLAGS =
#### END PROJECT SETTINGS ####

# These options can be overridden in config.mk
DISABLE_STAT = 1
DEBUG_BUILD = 0
BUILD_BENCH = 0
FORCE_DCHECK = 1

ifneq ("$(wildcard config.mk)","")
include config.mk
endif

ifneq (,$(findstring clang,$(CXX)))
COMPILE_FLAGS += -Wthread-safety -Wno-unused-private-field
endif

ifeq ($(DISABLE_STAT),1)
COMPILE_FLAGS += -D__FAAS_DISABLE_STAT
endif

ifeq ($(FORCE_DCHECK),1)
COMPILE_FLAGS += -DDCHECK_ALWAYS_ON
endif

# Function used to check variables. Use on the command line:
# make print-VARNAME
# Useful for debugging and adding features
print-%: ; @echo $*=$($*)

# Shell used in this makefile
# bash is used for 'echo -en'
SHELL = /bin/bash
# Clear built-in rules
.SUFFIXES:

# Verbose option, to output compile and link commands
export V := 0
export CMD_PREFIX := @
ifeq ($(V),1)
CMD_PREFIX :=
endif

COMPILE_FLAGS += $(INCLUDES)

ifeq ($(DEBUG_BUILD),1)
BUILD_NAME     = debug
COMPILE_FLAGS += $(DCOMPILE_FLAGS)
LINK_FLAGS    += $(DLINK_FLAGS)
else
BUILD_NAME     = release
COMPILE_FLAGS += $(RCOMPILE_FLAGS)
LINK_FLAGS    += $(RLINK_FLAGS)
endif

BUILD_PATH := build/$(BUILD_NAME)
BIN_PATH := bin/$(BUILD_NAME)

SROUCES = lib.cpp \
	$(SRC_PATH)/log/index_data.cpp \
	$(SRC_PATH)/log/cache.cpp \
	$(SRC_PATH)/base/init.cpp \
	$(SRC_PATH)/base/logging.cpp \
	$(SRC_PATH)/utils/debug.cpp \
	$(SRC_PATH)/utils/fs.cpp \
	$(SRC_PATH)/ipc/base.cpp \
	$(SRC_PATH)/ipc/shm_region.cpp

# Set the object file names, with the source directory stripped
# from the path, and the build path prepended in its place
OBJECTS = $(SROUCES:%.$(SRC_EXT)=$(BUILD_PATH)/%.o)
# Set the dependency files that will be used to add header dependencies
DEPS = $(OBJECTS:.o=.d)

TIME_FILE = $(dir $@).$(notdir $@)_time
START_TIME = date '+%s' > $(TIME_FILE)
END_TIME = read st < $(TIME_FILE) ; \
	$(RM) $(TIME_FILE) ; \
	st=$$((`date '+%s'` - $$st - 86400)) ; \
	echo `date -u -d @$$st '+%H:%M:%S'`

.PHONY: all
all: dirs
	@echo "Beginning $(BUILD_NAME) build"
	@$(START_TIME)
	@$(MAKE) library --no-print-directory
	@echo -n "Total build time of lib: "
	@$(END_TIME)

# Create the directories used in the build
.PHONY: dirs
dirs:
	@mkdir -p $(dir $(OBJECTS))
	@mkdir -p $(BIN_PATH)

# Removes all build files
.PHONY: clean
clean:
	@echo "Deleting directories"
	@$(RM) -r build bin

library: $(BIN_PATH)/libindex.so

# Link the shared library
$(BIN_PATH)/libindex.so: $(OBJECTS)
	@echo "Linking: $@"
	$(CMD_PREFIX)$(CXX) $^ $(LDFLAGS) $(LINK_FLAGS) -o $@

.SECONDARY: $(OBJECTS)

# Add dependency files, if they exist
-include $(DEPS)

# Source file rules
# After the first compilation they will be joined with the rules from the
# dependency files to provide header dependencies
$(BUILD_PATH)/%.o: %.$(SRC_EXT)
	@echo "Compiling: $< -> $@"
	$(CMD_PREFIX)$(CXX) $(CXXFLAGS) $(COMPILE_FLAGS) -MP -MMD -c $< -o $@
