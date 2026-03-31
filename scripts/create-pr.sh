#!/bin/bash
# Create GitHub PR for blog post submission
# Handles validation, branching, committing, and PR creation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if this is a dry run
DRY_RUN=false
if [[ " $@ " =~ " --dry " ]]; then
    DRY_RUN=true
fi

ARTICLE_FILE="$1"
if [ -z "$ARTICLE_FILE" ]; then
    echo -e "${RED}❌ Error: Please specify a markdown file${NC}"
    exit 1
fi

if [ ! -f "$ARTICLE_FILE" ]; then
    echo -e "${RED}❌ Error: File $ARTICLE_FILE not found${NC}"
    exit 1
fi

# Extract article metadata
BASENAME=$(basename "$ARTICLE_FILE" .md)
SLUG="$BASENAME"
BRANCH="blog/$SLUG"

# Extract title from frontmatter
TITLE=$(grep '^title:' "$ARTICLE_FILE" | sed 's/title: *["'\'']*\(.*\)["'\'']*$/\1/' | sed 's/^"//' | sed 's/"$//')
if [ -z "$TITLE" ]; then
    TITLE="$BASENAME"
fi

echo -e "${BLUE}📝 Processing blog post: $TITLE${NC}"
echo "   File: $ARTICLE_FILE"
echo "   Branch: $BRANCH"

# Validate article format first
echo -e "${BLUE}✅ Validating article format...${NC}"
if ! doppler run --project blog --config dev -- blog validate "$ARTICLE_FILE"; then
    echo -e "${RED}❌ Article validation failed. Please fix errors before submitting.${NC}"
    exit 1
fi

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}🔍 DRY RUN - No actual operations performed${NC}"
    echo ""
    echo "Would perform the following operations:"
    echo "  1. Create branch: $BRANCH"
    echo "  2. Add file: $ARTICLE_FILE"
    echo "  3. Commit with message: 'Add blog post: $TITLE'"
    echo "  4. Push branch to origin"
    echo "  5. Create PR with title: 'Blog Post: $TITLE'"
    echo ""
    exit 0
fi

# Check if we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo -e "${YELLOW}⚠️  Switching to main branch...${NC}"
    git checkout main
fi

# Pull latest changes
echo -e "${BLUE}⬇️  Pulling latest changes...${NC}"
git pull origin main 2>/dev/null || echo "Note: No remote commits to pull"

# Check if branch already exists
if git show-ref --quiet refs/heads/"$BRANCH"; then
    echo -e "${YELLOW}⚠️  Branch $BRANCH already exists. Switching to it...${NC}"
    git checkout "$BRANCH"
    echo -e "${BLUE}🔄 Updating existing branch...${NC}"
else
    echo -e "${BLUE}🌿 Creating new branch: $BRANCH${NC}"
    git checkout -b "$BRANCH"
fi

# Stage the article file
echo -e "${BLUE}📁 Adding file to git...${NC}"
git add "$ARTICLE_FILE"

# Check if there are any changes to commit
if git diff --cached --quiet; then
    echo -e "${YELLOW}⚠️  No changes detected in $ARTICLE_FILE${NC}"
    echo "File may already be committed. Checking for existing PR..."

    # Check if PR already exists
    if gh pr view "$BRANCH" --json url --jq '.url' 2>/dev/null; then
        echo -e "${GREEN}✅ PR already exists for this branch${NC}"
        exit 0
    else
        echo -e "${YELLOW}Creating PR for existing branch...${NC}"
    fi
else
    # Commit the changes
    COMMIT_MSG="Add blog post: $TITLE

- File: $ARTICLE_FILE
- Slug: $SLUG
- Auto-generated commit via blog CLI"

    echo -e "${BLUE}💾 Committing changes...${NC}"
    git commit -m "$COMMIT_MSG"
fi

# Push the branch
echo -e "${BLUE}⬆️  Pushing branch to GitHub...${NC}"
git push origin "$BRANCH"

# Create PR using GitHub CLI
echo -e "${BLUE}🔄 Creating GitHub PR...${NC}"

PR_BODY="## Blog Post Submission

**Title:** $TITLE
**File:** \`$ARTICLE_FILE\`
**Slug:** \`$SLUG\`

### What's included
- ✅ Markdown validated
- ✅ Frontmatter format checked
- ✅ Ready for review

### Next Steps
1. 👀 Review the content in the Files tab
2. 🚀 Preview will be automatically generated
3. ✅ Merge to auto-publish to Dev.to

---
*This PR was created automatically via the blog CLI \`submit\` command*"

# Create the PR
gh pr create \
    --title "Blog Post: $TITLE" \
    --body "$PR_BODY" \
    --assignee "@me" \
    --label "blog-post" \
    --base main \
    --head "$BRANCH"

# Get the PR URL
PR_URL=$(gh pr view "$BRANCH" --json url --jq '.url')

echo ""
echo -e "${GREEN}✅ Success! GitHub PR created${NC}"
echo -e "${BLUE}🔗 PR URL: ${NC}$PR_URL"
echo ""
echo "Next steps:"
echo "  1. 👀 Review your post at: $PR_URL"
echo "  2. 🚀 Preview will be available shortly"
echo "  3. ✅ Merge PR to publish to Dev.to"
echo ""
echo -e "${YELLOW}💡 Use 'blog pr-status $BASENAME' to check status anytime${NC}"

# Switch back to main branch
git checkout main