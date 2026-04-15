# Colab Audiobook Pipeline

This repository holds the public-safe part of the split Colab audiobook pipeline:

- `audiobook_pipeline_runtime_core_v2.py`: the remote runtime core downloaded by Colab at run time
- `generate_split_pipeline_files.py`: helper script used to generate the split files from the original notebook

## Recommended workflow

1. Keep `audiobook_pipeline_colab_loader_v2.ipynb` only in your local machine or Colab.
2. Push `audiobook_pipeline_runtime_core_v2.py` to GitHub.
3. Update `REMOTE_PIPELINE_URL` in the loader notebook to your GitHub Raw URL.
4. Run the loader notebook in Colab to always fetch the latest runtime code.

## Notes

- `audiobook_pipeline_colab_loader_v2.ipynb` is intentionally not committed because it may contain local credentials.
- Sensitive runtime values should stay in Colab or Supabase, not in the public runtime core.
- The original notebook `audiobook_pipeline_v1.ipynb` is kept for reference and was not overwritten.
