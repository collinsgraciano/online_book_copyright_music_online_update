import json
import re
import textwrap
from pathlib import Path
from pprint import pformat


ROOT = Path(__file__).resolve().parent
SOURCE_NOTEBOOK = ROOT / "audiobook_pipeline_v1.ipynb"
LOADER_NOTEBOOK = ROOT / "audiobook_pipeline_colab_loader_v2.ipynb"
RUNTIME_CORE = ROOT / "audiobook_pipeline_runtime_core_v2.py"
SENSITIVE_DEFAULT_KEYS = {
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "MODELSCOPE_TOKEN",
    "HF_DATASET_ZIP_URLS",
    "BUCKET_IDS",
    "HF_TOKEN",
}


def load_notebook():
    return json.loads(SOURCE_NOTEBOOK.read_text(encoding="utf-8"))


def get_cell_sources(nb):
    return ["".join(cell.get("source", [])) for cell in nb["cells"]]


def extract_config_variable_names(config_source):
    names = []
    seen = set()
    for line in config_source.splitlines():
        match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
        if not match:
            continue
        name = match.group(1)
        if name not in seen:
            names.append(name)
            seen.add(name)
    return names


def collect_default_runtime_config(config_source, config_variable_names):
    namespace = {}
    exec(config_source, {}, namespace)
    return {key: namespace[key] for key in config_variable_names if key in namespace}


def sanitize_default_runtime_config(default_config):
    sanitized = dict(default_config)
    for key in SENSITIVE_DEFAULT_KEYS:
        if key in sanitized:
            sanitized[key] = ""
    return sanitized


def remove_cell_title_line(source):
    lines = source.splitlines()
    if lines and lines[0].startswith("#@title"):
        lines = lines[1:]
    return "\n".join(lines).lstrip("\n")


def strip_trailing_top_level_print(source):
    lines = source.rstrip().splitlines()
    while lines:
        line = lines[-1].strip()
        if not line:
            lines.pop()
            continue
        if lines[-1].startswith("print("):
            lines.pop()
            continue
        break
    return "\n".join(lines).rstrip() + "\n"


def normalize_runtime_cell(source):
    source = remove_cell_title_line(source)
    source = strip_trailing_top_level_print(source)
    return source


def build_sync_music_function():
    return textwrap.dedent(
        """
        def sync_music_library_if_enabled():
            apply_music_download_runtime_overrides()

            if DOWNLOAD_FROM_BUCKETS:
                selected_method = str(HF_MUSIC_DOWNLOAD_METHOD or "datasets_zip_urls").strip().lower()
                if selected_method == "buckets":
                    return download_music_from_buckets()
                return download_music_from_dataset_urls()

            print("⏭️ 已关闭版权音乐自动同步。")
            return False
        """
    ).strip() + "\n"


def build_run_pipeline_function(main_cell_source):
    lines = main_cell_source.splitlines()
    while lines and (lines[0].startswith("#@title") or lines[0].startswith("from ") or lines[0].startswith("import ")):
        lines.pop(0)
    body = "\n".join(lines).rstrip()
    body = textwrap.indent(body, "    ")
    body = body.replace("    # ── 查询书籍 ──", "    sync_music_library_if_enabled()\n\n    # ── 查询书籍 ──", 1)
    body = body.replace("    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)", '    global supabase\n    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)', 1)

    footer = textwrap.indent(
        textwrap.dedent(
            """
            if not all_books:
                return {
                    "success": True,
                    "results": [],
                    "summary_path": "",
                    "stop_reason": "",
                    "successful_upload_count": 0,
                }

            return {
                "success": failed == 0 and partial == 0,
                "results": all_results,
                "summary_path": summary_path,
                "stop_reason": stop_reason,
                "successful_upload_count": successful_upload_count,
            }
            """
        ).strip(),
        "    ",
    )

    return (
        "def run_pipeline(runtime_config: dict | None = None):\n"
        "    apply_runtime_config(runtime_config)\n\n"
        f"{body}\n\n"
        f"{footer}\n"
    )


def build_runtime_core(cell_sources, default_config):
    music_source = normalize_runtime_cell(cell_sources[5])
    music_source = re.sub(
        r"\napply_music_download_runtime_overrides\(\)\n\nif DOWNLOAD_FROM_BUCKETS:.*?\nelse:\n    print\(\"⏭️ 已关闭版权音乐自动同步。\"\)\n?$",
        "\n" + build_sync_music_function(),
        music_source,
        flags=re.S,
    )

    tool_source = normalize_runtime_cell(cell_sources[6])
    bgm_source = normalize_runtime_cell(cell_sources[7])
    cover_source = normalize_runtime_cell(cell_sources[8])

    seo_source = normalize_runtime_cell(cell_sources[9])
    seo_source = re.sub(
        r"def normalize_modelscope_token_pool\(token_value\):.*?def auto_create_youtube_seo",
        "def auto_create_youtube_seo",
        seo_source,
        flags=re.S,
    )

    timestamp_source = normalize_runtime_cell(cell_sources[10])
    video_source = normalize_runtime_cell(cell_sources[11])
    upload_source = normalize_runtime_cell(cell_sources[12])
    book_source = normalize_runtime_cell(cell_sources[13])
    run_pipeline_source = build_run_pipeline_function(cell_sources[14])

    default_config_text = pformat(default_config, sort_dicts=False, width=120)

    parts = [
        '"""Remote runtime core for the Colab audiobook pipeline."""\n',
        "from __future__ import annotations\n\n",
        "from supabase import create_client\n\n",
        f"DEFAULT_RUNTIME_CONFIG = {default_config_text}\n",
        "supabase = None\n\n",
        textwrap.dedent(
            """
            def apply_runtime_config(runtime_config: dict | None = None):
                merged = dict(DEFAULT_RUNTIME_CONFIG)
                if runtime_config:
                    merged.update(runtime_config)

                if not str(merged.get("PROJECT_FLAG", "") or "").strip():
                    merged["PROJECT_FLAG"] = str(merged.get("YOUTUBE_CHANNEL_NAME", "") or "").strip()

                if not str(merged.get("MUSIC_DIR", "") or "").strip():
                    merged["MUSIC_DIR"] = str(merged.get("LOCAL_MUSIC_DIR", "") or "").strip()

                globals().update(merged)
                return merged


            apply_runtime_config()
            """
        ).strip()
        + "\n\n",
        music_source,
        "\n\n",
        tool_source,
        "\n\n",
        bgm_source,
        "\n\n",
        cover_source,
        "\n\n",
        seo_source,
        "\n\n",
        timestamp_source,
        "\n\n",
        video_source,
        "\n\n",
        upload_source,
        "\n\n",
        book_source,
        "\n\n",
        run_pipeline_source,
        "\n",
    ]
    return "".join(parts)


def make_code_cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source.splitlines()],
    }


def make_markdown_cell(source):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source.splitlines()],
    }


def build_loader_notebook(nb, config_variable_names):
    config_keys_literal = pformat(config_variable_names, width=120)
    remote_settings_source = textwrap.dedent(
        """
        #@title 5️⃣ 🌐 远端运行核心加载设置
        REMOTE_PIPELINE_URL = "https://raw.githubusercontent.com/<your-account>/<your-repo>/main/audiobook_pipeline_runtime_core_v2.py"  #@param {type:"string"}
        REMOTE_PIPELINE_LOCAL_PATH = "/content/_remote_pipeline/"  #@param {type:"string"}
        REMOTE_PIPELINE_FORCE_REFRESH = True  #@param {type:"boolean"}
        """
    ).strip()

    remote_run_source = textwrap.dedent(
        """
        #@title 6️⃣ 🚀 下载并运行远端核心
        import importlib.util
        import os
        import sys
        import time
        from pathlib import Path
        from urllib.parse import urlparse

        import requests

        RUNTIME_CONFIG_KEYS = __CONFIG_KEYS_LITERAL__


        def _ensure_remote_runtime_ready(remote_url, local_root, force_refresh=True):
            remote_url = str(remote_url or "").strip()
            if not remote_url:
                raise ValueError("REMOTE_PIPELINE_URL 不能为空。")
            if "<your-account>" in remote_url or "<your-repo>" in remote_url:
                raise ValueError("请先把 REMOTE_PIPELINE_URL 改成你自己的 GitHub Raw 地址。")

            local_root = Path(str(local_root or "/content/_remote_pipeline/").strip() or "/content/_remote_pipeline/")
            filename = os.path.basename(urlparse(remote_url).path) or "audiobook_pipeline_runtime_core_v2.py"
            if local_root.suffix == ".py":
                target_path = local_root
            else:
                target_path = local_root / filename

            target_path.parent.mkdir(parents=True, exist_ok=True)

            should_download = bool(force_refresh) or not target_path.exists() or target_path.stat().st_size == 0
            if should_download:
                download_url = remote_url
                if force_refresh:
                    separator = "&" if "?" in remote_url else "?"
                    download_url = f"{remote_url}{separator}t={int(time.time())}"

                response = requests.get(
                    download_url,
                    headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
                    timeout=120,
                )
                response.raise_for_status()
                target_path.write_text(response.text, encoding="utf-8")
                print(f"✅ 已下载远端运行核心到: {target_path}")
            else:
                print(f"♻️ 复用本地已下载的运行核心: {target_path}")

            return target_path


        def _collect_runtime_config_from_notebook_globals():
            runtime_config = {key: globals()[key] for key in RUNTIME_CONFIG_KEYS if key in globals()}
            runtime_config.update(
                {
                    key: value
                    for key, value in globals().items()
                    if key.isupper() and not key.startswith("_")
                }
            )
            return runtime_config


        remote_core_path = _ensure_remote_runtime_ready(
            REMOTE_PIPELINE_URL,
            REMOTE_PIPELINE_LOCAL_PATH,
            REMOTE_PIPELINE_FORCE_REFRESH,
        )

        module_name = f"audiobook_pipeline_runtime_core_v2_{int(time.time())}"
        spec = importlib.util.spec_from_file_location(module_name, remote_core_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"无法为远端核心创建 importlib spec: {remote_core_path}")
        remote_module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = remote_module
        spec.loader.exec_module(remote_module)

        runtime_config = _collect_runtime_config_from_notebook_globals()
        runtime_result = remote_module.run_pipeline(runtime_config=runtime_config)
        print("🎉 远端核心执行完成。")
        """
    ).replace("__CONFIG_KEYS_LITERAL__", config_keys_literal).strip()

    loader_markdown = textwrap.dedent(
        """
        ## Loader Notebook 说明

        这个新版 Notebook 只保留稳定配置和初始化操作，适合长期固定放在 Colab 中。

        ## 结构

        1. 安装依赖和 FFmpeg
        2. 配置所有运行参数
        3. 可选：生成 Supabase 建表 SQL
        4. 可选：同步全局共享云端配置
        5. 首次初始化 YouTube 授权
        6. 配置远端 GitHub Raw 运行核心地址
        7. 下载并运行远端核心

        ## 日常使用

        1. 运行安装依赖单元
        2. 检查配置参数
        3. 首次接入时执行 Supabase 建表和 YouTube 授权
        4. 把 `REMOTE_PIPELINE_URL` 改成你 GitHub 上的 Raw 文件地址
        5. 运行“下载并运行远端核心”

        ## 维护方式

        - 平时只改 GitHub 上的 `audiobook_pipeline_runtime_core_v2.py`
        - Colab 里的这个 loader notebook 一般不用重新上传
        - 每次运行时会重新下载最新核心脚本
        """
    ).strip()

    new_cells = list(nb["cells"][:5])
    new_cells.append(make_code_cell(remote_settings_source))
    new_cells.append(make_code_cell(remote_run_source))
    new_cells.append(make_markdown_cell(loader_markdown))

    new_notebook = dict(nb)
    new_notebook["cells"] = new_cells
    return new_notebook


def main():
    notebook = load_notebook()
    cell_sources = get_cell_sources(notebook)
    config_variable_names = extract_config_variable_names(cell_sources[1])
    default_config = collect_default_runtime_config(cell_sources[1], config_variable_names)
    default_config = sanitize_default_runtime_config(default_config)

    runtime_core_text = build_runtime_core(cell_sources, default_config)
    RUNTIME_CORE.write_text(runtime_core_text, encoding="utf-8")

    loader_notebook = build_loader_notebook(notebook, config_variable_names)
    LOADER_NOTEBOOK.write_text(json.dumps(loader_notebook, ensure_ascii=False, indent=1), encoding="utf-8")

    print(f"Generated: {RUNTIME_CORE.name}")
    print(f"Generated: {LOADER_NOTEBOOK.name}")


if __name__ == "__main__":
    main()
