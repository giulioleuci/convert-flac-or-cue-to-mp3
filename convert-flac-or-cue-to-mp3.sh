#!/bin/bash

# Audio to MP3 Converter with CUE Sheet Support
# Version 2.1.0 - With parallel processing and encoding fixes
# Converts FLAC/APE audio files and CUE sheets to individual MP3 files

# Don't use strict error handling to allow continuation on errors
set -uo pipefail
IFS=$'\n\t'

# Script configuration
readonly SCRIPT_NAME="$(basename "$0")"
readonly VERSION="2.1.0"
readonly DEFAULT_OUTPUT_DIR="MP3_Export"
readonly MP3_QUALITY="0"  # VBR ~245 kbps
readonly DEFAULT_PARALLEL_JOBS=8

# Color codes for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

# Global variables
ARTIST=""
ALBUM=""
GENRE=""
DISC=""
OUTPUT_DIR=""
TEMP_DIR=""
PARALLEL_JOBS="$DEFAULT_PARALLEL_JOBS"
PROCESSED_AUDIO_FILES=()
SUCCESS_COUNT=0
ERROR_COUNT=0
LOCK_FILE=""

# Set UTF-8 encoding - try multiple locales
if locale -a 2>/dev/null | grep -q "en_US.utf8"; then
    export LANG=en_US.utf8
    export LC_ALL=en_US.utf8
elif locale -a 2>/dev/null | grep -q "C.UTF-8"; then
    export LANG=C.UTF-8
    export LC_ALL=C.UTF-8
else
    export LANG=C
    export LC_ALL=C
fi

# Print colored message
print_message() {
    local color="$1"
    shift
    echo -e "${color}$*${NC}"
}

# Print error and exit
die() {
    print_message "$RED" "ERROR: $*" >&2
    cleanup_temp
    exit 1
}

# Print warning
warn() {
    print_message "$YELLOW" "WARNING: $*" >&2
}

# Print info
info() {
    print_message "$BLUE" "$*"
}

# Print success
success() {
    print_message "$GREEN" "✓ $*"
}

# Print failure
failure() {
    print_message "$RED" "✗ $*"
}

# Thread-safe counter increment
increment_success() {
    if [[ -n "$LOCK_FILE" ]] && [[ -f "$LOCK_FILE" ]]; then
        (
            flock 200
            local count=$(cat "$LOCK_FILE.success" 2>/dev/null || echo 0)
            count=$((count + 1))
            echo "$count" > "$LOCK_FILE.success"
        ) 200>"$LOCK_FILE.success.lock"
    else
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    fi
}

increment_error() {
    if [[ -n "$LOCK_FILE" ]] && [[ -f "$LOCK_FILE" ]]; then
        (
            flock 200
            local count=$(cat "$LOCK_FILE.error" 2>/dev/null || echo 0)
            count=$((count + 1))
            echo "$count" > "$LOCK_FILE.error"
        ) 200>"$LOCK_FILE.error.lock"
    else
        ERROR_COUNT=$((ERROR_COUNT + 1))
    fi
}

# Show usage
usage() {
    cat << EOF
Usage: $SCRIPT_NAME --artist "Artist Name" [OPTIONS]

Converts FLAC/APE audio files and CUE sheets to MP3 with metadata tagging.

Required:
  --artist "name"    Artist name for metadata

Optional:
  --album "name"     Album name for metadata
  --genre "name"     Genre for metadata
  --disc "N"         Disc number for metadata
  --output "dir"     Output directory (default: $DEFAULT_OUTPUT_DIR)
  --parallel N       Number of parallel conversion jobs (default: $DEFAULT_PARALLEL_JOBS)
  --help             Show this help message

Example:
  $SCRIPT_NAME --artist "Debussy" --album "Complete Works" --genre "Classical" --parallel 8

EOF
    exit 0
}

# Parse command line arguments
parse_arguments() {
    if [[ $# -eq 0 ]]; then
        usage
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --artist)
                ARTIST="$2"
                shift 2
                ;;
            --album)
                ALBUM="$2"
                shift 2
                ;;
            --genre)
                GENRE="$2"
                shift 2
                ;;
            --disc)
                DISC="$2"
                shift 2
                ;;
            --output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --parallel)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            --help|-h)
                usage
                ;;
            *)
                die "Unknown option: $1"
                ;;
        esac
    done

    # Validate required arguments
    if [[ -z "$ARTIST" ]]; then
        die "Artist name is required. Use --artist \"Artist Name\""
    fi

    # Set default output directory if not specified
    if [[ -z "$OUTPUT_DIR" ]]; then
        OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
    fi
}

# Check for required dependencies
check_dependencies() {
    local missing_deps=()

    info "Checking dependencies..."

    for cmd in ffmpeg cuebreakpoints shnsplit cueprint find; do
        if ! command -v "$cmd" &> /dev/null; then
            missing_deps+=("$cmd")
        fi
    done

    # iconv is optional but recommended
    if ! command -v iconv &> /dev/null; then
        warn "iconv not found - character encoding conversion may be limited"
    fi

    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        print_message "$RED" "Missing required dependencies: ${missing_deps[*]}"
        echo ""
        echo "Installation instructions:"
        echo "  Ubuntu/Debian: sudo apt-get install ffmpeg cuetools shntool"
        echo "  macOS: brew install ffmpeg cuetools shntool"
        echo "  Fedora: sudo dnf install ffmpeg cuetools shntool"
        exit 1
    fi

    success "All dependencies found"
}

# Create temporary directory
setup_temp_dir() {
    TEMP_DIR=$(mktemp -d) || die "Failed to create temporary directory"
    LOCK_FILE="$TEMP_DIR/lock"
    echo "0" > "$LOCK_FILE.success"
    echo "0" > "$LOCK_FILE.error"
    trap cleanup_temp EXIT
}

# Clean up temporary files
cleanup_temp() {
    if [[ -n "$TEMP_DIR" ]] && [[ -d "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
    fi
}

# Sanitize filename for cross-platform compatibility, preserving UTF-8 characters
sanitize_filename() {
    local filename="$1"

    # Remove or replace characters that are illegal in many filesystems.
    # Accented UTF-8 characters are preserved by removing the iconv transliteration.
    echo "$filename" | \
        sed 's/[<>:"|?*]/_/g' | \
        sed 's/\//_/g' | \
        sed 's/\\/_/g' | \
        sed 's/[[:cntrl:]]//g' | \
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
        sed 's/[[:space:]]\+/ /g'
}

# Get audio file referenced by CUE file
get_audio_file_for_cue() {
    local cue_file="$1"
    local cue_dir
    cue_dir="$(dirname "$cue_file")"
    local audio_file=""

    # First, check for same basename
    local base_name
    base_name="$(basename "$cue_file" .cue)"

    for ext in flac ape wav; do
        local test_file="$cue_dir/$base_name.$ext"
        if [[ -f "$test_file" ]]; then
            audio_file="$test_file"
            break
        fi
    done

    # If not found, parse CUE file for FILE directive
    if [[ -z "$audio_file" ]]; then
        local file_ref
        # Try different encodings
        for encoding in UTF-8 ISO-8859-1 WINDOWS-1252; do
            if command -v iconv &> /dev/null; then
                file_ref=$(iconv -f "$encoding" -t UTF-8 "$cue_file" 2>/dev/null | \
                    grep -E '^FILE ".*" (WAVE|APE|FLAC)' | \
                    head -1 | \
                    sed 's/^FILE "\(.*\)" .*/\1/' || true)
            else
                file_ref=$(grep -E '^FILE ".*" (WAVE|APE|FLAC)' "$cue_file" 2>/dev/null | \
                    head -1 | \
                    sed 's/^FILE "\(.*\)" .*/\1/' || true)
            fi

            if [[ -n "$file_ref" ]]; then
                break
            fi
        done

        if [[ -n "$file_ref" ]]; then
            # Handle relative and absolute paths
            if [[ "$file_ref" = /* ]]; then
                audio_file="$file_ref"
            else
                audio_file="$cue_dir/$file_ref"
            fi

            if [[ ! -f "$audio_file" ]]; then
                audio_file=""
            fi
        fi
    fi

    echo "$audio_file"
}

# Convert APE to WAV
convert_ape_to_wav() {
    local ape_file="$1"
    local wav_file="$2"

    info "Converting APE to WAV..."
    if ffmpeg -i "$ape_file" -acodec pcm_s16le "$wav_file" -y -loglevel error 2>&1; then
        return 0
    else
        return 1
    fi
}

# Extract metadata from CUE file with encoding handling
# Extract metadata from CUE file with encoding handling
get_cue_metadata() {
    local cue_file="$1"
    local track_num="$2"
    local field="$3"
    local result=""
    local temp_cue_file=""
    local cue_file_to_use="$cue_file"

    # Create a temporary UTF-8 version of the CUE file to handle encoding issues.
    # This is crucial because cueprint can misread non-UTF8 files.
    if command -v iconv &> /dev/null; then
        temp_cue_file=$(mktemp "$TEMP_DIR/temp_cue_XXXXXX.cue")
        # Attempt to convert from common legacy encodings to UTF-8.
        # WINDOWS-1252 is a common encoding for older CUE files.
        if iconv -f WINDOWS-1252 -t UTF-8//IGNORE "$cue_file" > "$temp_cue_file" 2>/dev/null || \
           iconv -f ISO-8859-1 -t UTF-8//IGNORE "$cue_file" > "$temp_cue_file" 2>/dev/null; then
            cue_file_to_use="$temp_cue_file"
        else
            # If conversion fails, use the original file and remove the temp.
            rm -f "$temp_cue_file"
            temp_cue_file=""
        fi
    fi

    # Extract metadata using the (potentially converted) CUE file.
    result=$(cueprint -n "$track_num" -t "%$field" "$cue_file_to_use" 2>/dev/null || true)

    # Clean up the temporary CUE file if it was used.
    if [[ -n "$temp_cue_file" ]] && [[ -f "$temp_cue_file" ]]; then
        rm -f "$temp_cue_file"
    fi

    echo "$result"
}

# Get total tracks from CUE file
get_total_tracks() {
    local cue_file="$1"
    local total
    total=$(cueprint -d '%N' "$cue_file" 2>/dev/null || echo "0")
    echo "$total"
}

# Convert single track to MP3 (for parallel processing)
convert_track_to_mp3() {
    local track_file="$1"
    local output_file="$2"
    local track_artist="$3"
    local track_album="$4"
    local track_title="$5"
    local track_num="$6"
    local total_tracks="$7"

    # Prepare ffmpeg arguments
    local ffmpeg_args=()
    ffmpeg_args+=(-i "$track_file")
    ffmpeg_args+=(-q:a "$MP3_QUALITY")

    # Add metadata tags if not empty
    [[ -n "$track_artist" ]] && ffmpeg_args+=(-metadata "artist=$track_artist")
    [[ -n "$track_album" ]] && ffmpeg_args+=(-metadata "album=$track_album")
    [[ -n "$track_title" ]] && ffmpeg_args+=(-metadata "title=$track_title")
    [[ -n "$GENRE" ]] && ffmpeg_args+=(-metadata "genre=$GENRE")
    [[ -n "$DISC" ]] && ffmpeg_args+=(-metadata "disc=$DISC")

    if [[ -n "$track_num" ]] && [[ -n "$total_tracks" ]]; then
        ffmpeg_args+=(-metadata "track=$track_num/$total_tracks")
    fi

    # Convert to MP3
    if ffmpeg "${ffmpeg_args[@]}" "$output_file" -y -loglevel error 2>&1; then
        success "$(basename "$output_file")"
        increment_success
        return 0
    else
        failure "Failed: $(basename "$output_file")"
        increment_error
        return 1
    fi
}

# Export functions for parallel execution
export -f convert_track_to_mp3 success failure increment_success increment_error print_message sanitize_filename

# Process CUE file and split audio
process_cue_file() {
    local cue_file="$1"
    local audio_file="$2"
    local output_subdir="$3"

    info "Processing CUE: $(basename "$cue_file")"

    # Create output subdirectory
    local full_output_dir="$OUTPUT_DIR"
    if [[ -n "$output_subdir" ]]; then
        full_output_dir="$OUTPUT_DIR/$output_subdir"
    fi
    mkdir -p "$full_output_dir"

    # Prepare audio file for processing
    local process_audio_file="$audio_file"
    local temp_wav=""

    # Convert APE to WAV if necessary
    if [[ "${audio_file##*.}" == "ape" ]]; then
        temp_wav="$TEMP_DIR/temp_audio_$$.wav"
        if ! convert_ape_to_wav "$audio_file" "$temp_wav"; then
            failure "Failed to convert APE file: $audio_file"
            increment_error
            return 1
        fi
        process_audio_file="$temp_wav"
    fi

    # Get total tracks
    local total_tracks
    total_tracks=$(get_total_tracks "$cue_file")

    if [[ "$total_tracks" -eq 0 ]]; then
        failure "No tracks found in CUE file"
        increment_error
        return 1
    fi

    # Split audio file using cue
    info "Splitting audio into $total_tracks tracks..."

    # Create temporary directory for split files
    local split_dir="$TEMP_DIR/split_$$"
    mkdir -p "$split_dir"

    # Split the audio file
    if ! shnsplit -f "$cue_file" -t "%n" -o wav -d "$split_dir" -O never "$process_audio_file" 2>/dev/null; then
        failure "Failed to split audio file"
        increment_error
        rm -rf "$split_dir"
        [[ -n "$temp_wav" ]] && rm -f "$temp_wav"
        return 1
    fi

    # Process tracks
    info "Converting $total_tracks tracks to MP3 (using $PARALLEL_JOBS parallel jobs)..."

    # Create a temporary file for the jobs
    local jobs_file="$TEMP_DIR/jobs_$$.txt"

    local track_num=1
    while [[ $track_num -le $total_tracks ]]; do
        local track_file
        track_file=$(printf "%s/%02d.wav" "$split_dir" "$track_num")

        if [[ ! -f "$track_file" ]]; then
            warn "Track $track_num not found, skipping"
            ((track_num++))
            continue
        fi

        # Get metadata from CUE
        local track_title
        track_title=$(get_cue_metadata "$cue_file" "$track_num" "t")

        # Use CUE metadata with command line overrides
        local track_artist="${ARTIST}"
        if [[ -z "$track_artist" ]]; then
            track_artist=$(get_cue_metadata "$cue_file" "$track_num" "p")
            if [[ -z "$track_artist" ]]; then
                track_artist=$(get_cue_metadata "$cue_file" "0" "p")
            fi
        fi

        local track_album="${ALBUM}"
        if [[ -z "$track_album" ]]; then
            track_album=$(get_cue_metadata "$cue_file" "0" "T")
        fi

        # Generate output filename
        local output_filename
        if [[ -n "$track_title" ]]; then
            local safe_title
            safe_title=$(sanitize_filename "$track_title")
            output_filename=$(printf "%02d - %s.mp3" "$track_num" "$safe_title")
        else
            output_filename=$(printf "%02d - Track %02d.mp3" "$track_num" "$track_num")
        fi

        local output_file="$full_output_dir/$output_filename"

        # Add job to the list
        echo "$track_file|$output_file|$track_artist|$track_album|$track_title|$track_num|$total_tracks" >> "$jobs_file"

        ((track_num++))
    done

    # Process all jobs in parallel
    if [[ -f "$jobs_file" ]]; then
        # Export necessary variables
        export GENRE DISC LOCK_FILE RED GREEN YELLOW BLUE NC MP3_QUALITY

        # Process the jobs
        cat "$jobs_file" | while IFS='|' read -r track_file output_file track_artist track_album track_title track_num total_tracks; do
            echo "$track_file|$output_file|$track_artist|$track_album|$track_title|$track_num|$total_tracks"
        done | xargs -P "$PARALLEL_JOBS" -I {} bash -c '
            IFS="|" read -r track_file output_file track_artist track_album track_title track_num total_tracks <<< "{}"
            convert_track_to_mp3 "$track_file" "$output_file" "$track_artist" "$track_album" "$track_title" "$track_num" "$total_tracks"
        '

        rm -f "$jobs_file"
    fi

    # Clean up
    rm -rf "$split_dir"
    [[ -n "$temp_wav" ]] && rm -f "$temp_wav"

    return 0
}

# Process standalone audio file
process_standalone_audio() {
    local audio_file="$1"
    local output_subdir="$2"

    # Check if already processed by CUE
    local skip_file=false
    for processed_file in "${PROCESSED_AUDIO_FILES[@]}"; do
        if [[ "$audio_file" == "$processed_file" ]]; then
            skip_file=true
            break
        fi
    done

    if [[ "$skip_file" == "true" ]]; then
        return 0
    fi

    info "Processing standalone: $(basename "$audio_file")"

    # Determine output path
    local output_dir="$OUTPUT_DIR"
    if [[ -n "$output_subdir" ]]; then
        output_dir="$OUTPUT_DIR/$output_subdir"
    fi
    mkdir -p "$output_dir"

    # Generate output filename
    local base_name
    base_name="$(basename "$audio_file" | sed 's/\.[^.]*$//')"
    local safe_name
    safe_name=$(sanitize_filename "$base_name")
    local output_file="$output_dir/${safe_name}.mp3"

    # Convert to MP3
    convert_track_to_mp3 "$audio_file" "$output_file" "$ARTIST" "$ALBUM" "" "" ""
}

# Export for parallel processing
export -f process_standalone_audio

# Main processing function
main() {
    parse_arguments "$@"
    check_dependencies
    setup_temp_dir

    # Create output directory
    mkdir -p "$OUTPUT_DIR"

    print_message "$GREEN" "\n=== Audio to MP3 Converter v$VERSION ==="
    info "Artist: $ARTIST"
    [[ -n "$ALBUM" ]] && info "Album: $ALBUM"
    [[ -n "$GENRE" ]] && info "Genre: $GENRE"
    [[ -n "$DISC" ]] && info "Disc: $DISC"
    info "Output: $OUTPUT_DIR"
    info "Parallel Jobs: $PARALLEL_JOBS"
    echo ""

    # Find all CUE files
    info "Scanning for CUE files..."
    local cue_files=()
    while IFS= read -r -d '' file; do
        cue_files+=("$file")
    done < <(find . -type f -name "*.cue" -print0 2>/dev/null)

    if [[ ${#cue_files[@]} -gt 0 ]]; then
        info "Found ${#cue_files[@]} CUE file(s)"
        echo ""

        # Process each CUE file
        for cue_file in "${cue_files[@]}"; do
            # Get associated audio file
            local audio_file
            audio_file=$(get_audio_file_for_cue "$cue_file")

            if [[ -z "$audio_file" ]]; then
                failure "No audio file found for: $(basename "$cue_file")"
                increment_error
                continue
            fi

            # Mark audio file as processed
            PROCESSED_AUDIO_FILES+=("$audio_file")

            # Determine output subdirectory based on source location
            local subdir
            subdir="$(dirname "$cue_file" | sed 's|^\./||')"
            if [[ "$subdir" == "." ]]; then
                subdir=""
            fi

            # Process the CUE file
            process_cue_file "$cue_file" "$audio_file" "$subdir" || true
            echo ""
        done
    else
        info "No CUE files found"
        echo ""
    fi

    # Find all standalone FLAC/APE files
    info "Scanning for standalone audio files..."
    local audio_files=()
    while IFS= read -r -d '' file; do
        audio_files+=("$file")
    done < <(find . -type f \( -name "*.flac" -o -name "*.ape" \) -print0 2>/dev/null)

    if [[ ${#audio_files[@]} -gt 0 ]]; then
        info "Found ${#audio_files[@]} audio file(s)"
        echo ""

        # Process standalone files
        # Export necessary variables
        export OUTPUT_DIR ARTIST ALBUM GENRE DISC LOCK_FILE RED GREEN YELLOW BLUE NC MP3_QUALITY
        export -a PROCESSED_AUDIO_FILES

        # Create a temporary file for standalone jobs
        local standalone_jobs="$TEMP_DIR/standalone_jobs.txt"

        for audio_file in "${audio_files[@]}"; do
            # Check if already processed
            local skip_file=false
            for processed_file in "${PROCESSED_AUDIO_FILES[@]}"; do
                if [[ "$audio_file" == "$processed_file" ]]; then
                    skip_file=true
                    break
                fi
            done

            if [[ "$skip_file" == "false" ]]; then
                local subdir
                subdir="$(dirname "$audio_file" | sed 's|^\./||')"
                if [[ "$subdir" == "." ]]; then
                    subdir=""
                fi
                echo "$audio_file|$subdir" >> "$standalone_jobs"
            fi
        done

        # Process standalone files in parallel
        if [[ -f "$standalone_jobs" ]]; then
            cat "$standalone_jobs" | xargs -P "$PARALLEL_JOBS" -I {} bash -c '
                IFS="|" read -r audio_file subdir <<< "{}"
                process_standalone_audio "$audio_file" "$subdir"
            '
            rm -f "$standalone_jobs"
        fi
    else
        info "No standalone audio files found"
    fi

    # Read final counts from lock files
    if [[ -f "$LOCK_FILE.success" ]]; then
        SUCCESS_COUNT=$(cat "$LOCK_FILE.success" 2>/dev/null || echo 0)
    fi
    if [[ -f "$LOCK_FILE.error" ]]; then
        ERROR_COUNT=$(cat "$LOCK_FILE.error" 2>/dev/null || echo 0)
    fi

    # Print summary
    echo ""
    print_message "$GREEN" "=== Conversion Complete ==="
    success "Successfully converted: $SUCCESS_COUNT file(s)"
    if [[ $ERROR_COUNT -gt 0 ]]; then
        failure "Failed to convert: $ERROR_COUNT file(s)"
    fi
    info "Output directory: $OUTPUT_DIR"

    # Cleanup is handled by trap
}

# Run main function
main "$@"
