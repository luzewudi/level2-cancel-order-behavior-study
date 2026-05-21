"""
邢不行｜策略分享会
股票量化策略框架𝓟𝓻𝓸

版权所有 ©️ 邢不行
微信: xbx1717

本代码仅供个人学习使用，未经授权不得复制、修改或用于商业用途。

Author: 邢不行
"""

import os
from pathlib import Path

# 通过当前文件的位置，获取项目根目录（当前项目文件夹）
# 原始版本多退了一层目录，导致日志等输出路径落在了当前项目的上一级目录
# 这里只保留到 “当前文件的上两级目录”，即项目根目录本身
PROJECT_ROOT = os.path.abspath(os.path.join(__file__, os.path.pardir, os.path.pardir))


# ====================================================================================================
# ** 功能函数 **
# - get_folder_by_root: 获取基于某一个地址的绝对路径
# - get_folder_path: 获取相对于项目根目录的，文件夹的绝对路径
# - get_file_path: 获取相对于项目根目录的，文件的绝对路径
# ====================================================================================================
def get_folder_by_root(root, *paths, auto_create=True) -> str:
    """
    获取基于某一个地址的绝对路径
    :param root: 相对的地址，默认为运行脚本同目录
    :param paths: 路径
    :param auto_create: 是否自动创建需要的文件夹们
    :return: 绝对路径
    """
    _full_path = os.path.join(root, *paths)
    if auto_create and (not os.path.exists(_full_path)):  # 判断文件夹是否存在
        try:
            os.makedirs(_full_path)  # 不存在则创建
        except FileExistsError:
            pass  # 并行过程中，可能造成冲突
    return str(_full_path)


def get_folder_path(*paths, auto_create=True, as_path_type=True) -> str | Path:
    """
    获取相对于项目根目录的，文件夹的绝对路径
    :param paths: 文件夹路径
    :param auto_create: 是否自动创建
    :param as_path_type: 是否返回Path对象
    :return: 文件夹绝对路径
    """
    _p = get_folder_by_root(PROJECT_ROOT, *paths, auto_create=auto_create)
    if as_path_type:
        return Path(_p)
    return _p


def get_file_path(*paths, auto_create=True, as_path_type=True) -> str | Path:
    """
    获取相对于项目根目录的，文件的绝对路径
    :param paths: 文件路径
    :param auto_create: 是否自动创建
    :param as_path_type: 是否返回Path对象
    :return: 文件绝对路径
    """
    parent = get_folder_path(*paths[:-1], auto_create=auto_create, as_path_type=True)
    _p = parent / paths[-1]
    if as_path_type:
        return _p
    return str(_p)
