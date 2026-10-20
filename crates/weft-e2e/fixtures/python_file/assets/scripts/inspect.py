import urllib.request
assert "__weft_image__" not in photo, "the marker arrives unwrapped"
with urllib.request.urlopen(photo["internalUrl"], timeout=30) as response:
    fetched = response.read()
return {
    "body": {
        "filename": photo["filename"],
        "mimeType": photo["mimeType"],
        "fetched": len(fetched),
        "declared": photo["sizeBytes"],
        "externalUrl": photo["externalUrl"],
        "public": photo["public"],
    },
    "same": photo,
}
