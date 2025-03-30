# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all
from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

# List of required dependencies
added_files = [
    ('src/api/api.py', '.'),
    ('src/core/manager.py', '.'),
    ('src/schema/schema.py', '.'),
    ('src/utils/utils.py', '.')
]

# Hidden imports for web framework and server
hidden_imports = [
    'uvicorn',
    'fastapi',
    'httptools',  # Often needed with uvicorn
    'uvloop',     # Optional, but common with uvicorn
    'websockets',  # Often used with FastAPI
    'sqlalchemy',
    'pydantic'
]

hidden_imports.extend(collect_submodules('asyncpg'))

a = Analysis(
    ['src/core/main.py'],
    pathex=[],
    binaries=[],
    datas=added_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='bgdb',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='bgdb'
)
