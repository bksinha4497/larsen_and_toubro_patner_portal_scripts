# compress pdf files with text and images
find . -type f -iname "*.pdf" -print0 | while IFS= read -r -d '' f; do
  echo "🟡 Trying to compress: $f"

  # Create temporary file
  tmp="${f%.*}_compressed.pdf"

  # Run Ghostscript directly
  if gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/screen \
        -dNOPAUSE -dQUIET -dBATCH -sOutputFile="$tmp" "$f"; then
    mv "$tmp" "$f"
    echo "✅ Compressed: $f"
  else
    echo "⚠️ Skipped (error or permission issue): $f"
    rm -f "$tmp"
  fi
done
