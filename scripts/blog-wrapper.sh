#!/bin/bash
# Blog CLI Wrapper - GitHub PR-based publishing workflow
# Replaces direct publishing with PR-based workflow

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

case "$1" in
  "submit")
    echo -e "${BLUE}📝 Creating PR for blog post...${NC}"
    if [ -z "$2" ]; then
      echo -e "${RED}❌ Error: Please specify a markdown file${NC}"
      echo "Usage: blog submit <file.md> [--dry]"
      exit 1
    fi

    # Check if file exists in posts/ directory
    if [[ "$2" == posts/* ]]; then
      ARTICLE_FILE="$2"
    else
      ARTICLE_FILE="posts/$2"
    fi

    if [ ! -f "$ARTICLE_FILE" ]; then
      echo -e "${RED}❌ Error: File $ARTICLE_FILE not found${NC}"
      exit 1
    fi

    # Call PR creation script
    "$SCRIPT_DIR/create-pr.sh" "$ARTICLE_FILE" "${@:3}"
    ;;

  "pr-status"|"status")
    echo -e "${BLUE}📊 Checking PR and publication status...${NC}"
    if [ -n "$2" ]; then
      "$SCRIPT_DIR/check-pr-status.sh" "$2"
    else
      # Show status for all posts
      "$SCRIPT_DIR/check-pr-status.sh"
    fi
    ;;

  "validate")
    echo -e "${BLUE}✅ Validating blog post format...${NC}"
    if [ -z "$2" ]; then
      echo -e "${RED}❌ Error: Please specify a markdown file${NC}"
      exit 1
    fi

    # Check if file exists in posts/ directory
    if [[ "$2" == posts/* ]]; then
      ARTICLE_FILE="$2"
    else
      ARTICLE_FILE="posts/$2"
    fi

    # Pass through to original blog CLI
    doppler run --project blog --config dev -- blog validate "$ARTICLE_FILE"
    ;;

  "publish")
    echo -e "${RED}❌ Direct publishing is disabled for safety.${NC}"
    echo -e "${YELLOW}💡 Use 'blog submit <file.md>' to create a PR instead.${NC}"
    echo ""
    echo "New workflow:"
    echo "  1. blog submit <file.md>  # Creates GitHub PR"
    echo "  2. Review PR on GitHub    # Preview and collaborate"
    echo "  3. Merge PR              # Auto-publishes to Dev.to"
    exit 1
    ;;

  "help"|"--help"|"-h")
    echo -e "${GREEN}📚 Blog CLI - GitHub PR-based Publishing${NC}"
    echo ""
    echo "Available commands:"
    echo -e "  ${BLUE}submit${NC}      Create GitHub PR for blog post"
    echo -e "  ${BLUE}validate${NC}     Validate markdown format"
    echo -e "  ${BLUE}pr-status${NC}    Check PR and publication status"
    echo -e "  ${BLUE}help${NC}         Show this help message"
    echo ""
    echo "Examples:"
    echo "  blog submit my-post.md       # Create PR for new post"
    echo "  blog submit my-post.md --dry # Preview PR without creating"
    echo "  blog validate my-post.md     # Check format before submitting"
    echo "  blog pr-status my-post.md    # Check PR and publish status"
    echo ""
    echo -e "${YELLOW}Note: Direct publishing is disabled. All posts go through GitHub PR workflow.${NC}"
    ;;

  *)
    echo -e "${RED}❌ Unknown command: $1${NC}"
    echo -e "${YELLOW}💡 Run 'blog help' to see available commands.${NC}"
    exit 1
    ;;
esac