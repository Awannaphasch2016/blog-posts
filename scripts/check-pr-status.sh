#!/bin/bash
# Check PR and publication status for blog posts
# Usage: check-pr-status.sh [filename]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

BASENAME="$1"

if [ -n "$BASENAME" ]; then
    # Check specific file
    if [[ "$BASENAME" == *.md ]]; then
        BASENAME="${BASENAME%.md}"
    fi

    ARTICLE_FILE="posts/$BASENAME.md"
    BRANCH="blog/$BASENAME"

    echo -e "${BLUE}📊 Status for: $BASENAME${NC}"
    echo "   File: $ARTICLE_FILE"
    echo "   Branch: $BRANCH"
    echo ""

    # Check if file exists
    if [ ! -f "$ARTICLE_FILE" ]; then
        echo -e "${RED}❌ File not found: $ARTICLE_FILE${NC}"
        exit 1
    fi

    # Check branch status
    if git show-ref --quiet refs/heads/"$BRANCH"; then
        echo -e "${BLUE}🌿 Local branch: ${GREEN}exists${NC}"
    else
        echo -e "${BLUE}🌿 Local branch: ${YELLOW}not found${NC}"
    fi

    # Check remote branch
    if git ls-remote --heads origin "$BRANCH" | grep -q "$BRANCH"; then
        echo -e "${BLUE}☁️  Remote branch: ${GREEN}exists${NC}"
    else
        echo -e "${BLUE}☁️  Remote branch: ${YELLOW}not found${NC}"
    fi

    # Check PR status
    echo ""
    echo -e "${BLUE}📋 Checking GitHub PR status...${NC}"

    if PR_INFO=$(gh pr view "$BRANCH" --json state,url,title,number 2>/dev/null); then
        STATE=$(echo "$PR_INFO" | jq -r '.state')
        URL=$(echo "$PR_INFO" | jq -r '.url')
        TITLE=$(echo "$PR_INFO" | jq -r '.title')
        NUMBER=$(echo "$PR_INFO" | jq -r '.number')

        case "$STATE" in
            "OPEN")
                echo -e "   Status: ${YELLOW}Open PR #$NUMBER${NC}"
                echo "   URL: $URL"
                echo "   Title: $TITLE"
                echo ""
                echo -e "${YELLOW}💡 Next steps:${NC}"
                echo "   1. Review PR content"
                echo "   2. Check preview in PR comments"
                echo "   3. Merge to auto-publish to Dev.to"
                ;;
            "MERGED")
                echo -e "   Status: ${GREEN}Merged PR #$NUMBER${NC}"
                echo "   URL: $URL"
                echo "   Title: $TITLE"
                echo ""
                echo -e "${GREEN}✅ This post should be published to Dev.to${NC}"

                # Check if published to Dev.to by looking at Supabase tracking
                echo ""
                echo -e "${BLUE}📤 Checking publication status...${NC}"
                if doppler run --project blog --config dev -- blog status "$ARTICLE_FILE" --json 2>/dev/null | grep -q "published"; then
                    echo -e "   Dev.to: ${GREEN}Published${NC}"
                else
                    echo -e "   Dev.to: ${YELLOW}Status unclear - check manually${NC}"
                fi
                ;;
            "CLOSED")
                echo -e "   Status: ${RED}Closed PR #$NUMBER${NC}"
                echo "   URL: $URL"
                echo "   Title: $TITLE"
                echo ""
                echo -e "${RED}❌ PR was closed without merging${NC}"
                ;;
        esac
    else
        echo -e "   Status: ${RED}No PR found${NC}"
        echo ""
        echo -e "${YELLOW}💡 To create a PR:${NC}"
        echo "   blog submit $BASENAME.md"
    fi
else
    # Show status for all posts
    echo -e "${BLUE}📊 Blog Post Status Overview${NC}"
    echo ""

    # Find all markdown files in posts/
    if [ -d "posts" ]; then
        for file in posts/*.md; do
            if [ -f "$file" ]; then
                BASENAME=$(basename "$file" .md)
                BRANCH="blog/$BASENAME"

                printf "%-30s" "$BASENAME"

                # Check PR status
                if PR_STATE=$(gh pr view "$BRANCH" --json state --jq '.state' 2>/dev/null); then
                    case "$PR_STATE" in
                        "OPEN")
                            echo -e "${YELLOW}PR Open${NC}"
                            ;;
                        "MERGED")
                            echo -e "${GREEN}Merged/Published${NC}"
                            ;;
                        "CLOSED")
                            echo -e "${RED}PR Closed${NC}"
                            ;;
                    esac
                else
                    echo -e "${YELLOW}No PR${NC}"
                fi
            fi
        done
    else
        echo -e "${RED}❌ No posts/ directory found${NC}"
    fi

    echo ""
    echo -e "${BLUE}💡 Usage:${NC}"
    echo "   blog pr-status <filename>     # Check specific post"
    echo "   blog pr-status               # Show all posts overview"
    echo "   blog submit <filename>       # Create PR for new post"
fi