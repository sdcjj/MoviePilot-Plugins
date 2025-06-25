import csv
import os
from pathlib import Path
import threading
import time
from typing import List, Tuple, Dict, Any
from urllib.parse import urlparse

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from helper.downloader import DownloaderHelper
from transmission_rpc import Client as TransmissionClient
from qbittorrentapi import Client as QbittorrentClient

lock = threading.Lock()


class GreenLeaf(_PluginBase):
    # 插件名称
    plugin_name = "绿叶辅种插件"
    # 插件描述
    plugin_desc = "让绿叶生长"
    # 插件图标
    plugin_icon = "Vscode_A.png"
    # 插件版本
    plugin_version = "1.1.3"
    # 插件作者
    plugin_author = "xingxing"
    # 作者主页
    author_url = "https://github.com/sdcjj"
    # 插件配置项ID前缀
    plugin_config_prefix = "greenleaf_"
    # 加载顺序
    plugin_order = 8
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    _notify = None
    _cron = None
    _seed_domain = None
    _seed_passkey = None
    _seed_path = None
    _seed_path_replace = None
    _default_downloader = None
    _match_torrent_id_col = None
    _downloader_helper = None
    _seed_count = 0
    _seed_delay = 5
    _torr_suff = ["-RL", "-RL4B"]
    _torrent_data = {}
    _error_caches = []
    _success_caches = set()
    _seed_downloaders = []

    def init_plugin(self, config: dict = None):
        """初始化"""
        logger.info("初始化 GreenLeaf")
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._cron = config.get("cron")
            self._seed_domain = config.get("seeddomain")
            self._seed_passkey = config.get("seedpasskey")
            self._seed_path = config.get("seedpath")
            self._seed_path_replace = config.get("seedpathreplace")
            self._default_downloader = config.get("defaultdownloader")
            self._match_torrent_id_col = config.get("matchtorrentidcol")
            self._seed_delay = config.get("seeddelay")
            self._seed_downloaders = config.get("seeddownloaders")

        if not self._enabled:
            self._torrent_data.clear()
            self._error_caches.clear()
            self._success_caches.clear()
            logger.info("停用 GreenLeaf 完毕")
            return

        self._downloader_helper = DownloaderHelper()
        if self._enabled and self.get_state():
            try:
                self._torrent_data.clear()
                self._error_caches.clear()
                self._success_caches.clear()
                self.__load_torrent_data()
                self.__init_success_caches()
                logger.info("初始化 GreenLeaf 完毕 等待定时任务执行")
            except Exception as e:
                logger.error(f"初始化 GreenLeaf 失败：{str(e)}")
                self._enabled = False

    def get_state(self) -> bool:
        if (
            self._enabled
            and self._cron
            and self._seed_domain
            and self._seed_passkey
            and self._match_torrent_id_col
        ):
            return True
        else:
            return False

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self.get_state():
            return [
                {
                    "id": "GreenLeaf",
                    "name": "让绿叶生长",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__seed_torrents,
                    "kwargs": {},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "0 */12 * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VNumberInput",
                                        "props": {
                                            "min": "1",
                                            "model": "seeddelay",
                                            "label": "辅种间隔(秒)",
                                            "placeholder": "辅种间隔",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 8, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "seeddomain",
                                            "label": "辅种站点域名",
                                            "placeholder": "https://xxx.xx/",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 8, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "seedpasskey",
                                            "label": "辅种站点passkey",
                                            "placeholder": "辅种站点passkey",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 8, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "chips": True,
                                            "multiple": False,
                                            "model": "matchtorrentidcol",
                                            "label": "辅种站点的关键字",
                                            "items": ["torrent_id_1", "torrent_id_2"],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "chips": True,
                                            "multiple": True,
                                            "model": "seeddownloaders",
                                            "label": "按下载器辅种",
                                            "items": [
                                                {
                                                    "title": config.name,
                                                    "value": config.name,
                                                }
                                                for config in DownloaderHelper()
                                                .get_configs()
                                                .values()
                                            ],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "seedpath",
                                            "label": "按目录辅种",
                                            "placeholder": "媒体文件所在路径,适合种子已删除的场景",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "chips": True,
                                            "multiple": False,
                                            "model": "defaultdownloader",
                                            "label": "指定目录的下载器",
                                            "items": [
                                                {
                                                    "title": config.name,
                                                    "value": config.name,
                                                }
                                                for config in DownloaderHelper()
                                                .get_configs()
                                                .values()
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "seedpathreplace",
                                            "label": "下载目录映射",
                                            "placeholder": "指定目录|下载目录",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "cron": "22 */12 * * *",
            "seeddomain": "",
            "seedpasskey": "",
            "seedpath": "",
            "seedpathreplace": "",
            "matchtorrentidcol": "",
            "seeddelay": 5,
            "seeddownloaders": "",
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        try:
            self._torrent_data.clear()
            self._error_caches.clear()
            self._success_caches.clear()
            logger.info("停止 GreenLeaf 完毕")
        except Exception as e:
            logger.error(f"stop_service error {str(e)}")

    def __seed_torrents(self):
        """辅种总入口"""
        logger.info("__seed_torrents 开始辅种")
        try:
            qb_downloaders = self._downloader_helper.get_services(
                type_filter="qbittorrent"
            )
            for downloader in qb_downloaders:
                if downloader not in self._seed_downloaders:
                    continue
                inst = self._downloader_helper.get_service(name=downloader)
                torrs = inst.instance.get_completed_torrents()
                for torrent in torrs:
                    self.__seed_torrent(
                        downloader,
                        torrent.get("name"),
                        torrent.get("save_path"),
                        [torrent.get("tracker")],
                    )
                logger.info(f"qb  完毕 {len(torrs)}")
        except Exception as e:
            logger.error(f"qb 辅种失败 {str(e)}")

        try:
            tr_downloaders = self._downloader_helper.get_services(
                type_filter="transmission"
            )
            for downloader in tr_downloaders:
                if downloader not in self._seed_downloaders:
                    continue
                inst = self._downloader_helper.get_service(name=downloader)
                torrs = inst.instance.get_completed_torrents()
                for torrent in torrs:
                    self.__seed_torrent(
                        downloader,
                        torrent.name,
                        torrent.download_dir,
                        torrent.tracker_list,
                    )
                logger.debug(f"tr  完毕 {len(torrs)}")
        except Exception as e:
            logger.error(f"tr 辅种失败 {str(e)}")

        try:
            self.__seed_torrents_path()
        except Exception as e:
            logger.error(f"根据文件路径辅种 辅种失败 {str(e)}")

        logger.info(
            f"__seed_torrents 辅种完毕 共辅种:{self._seed_count} 辅种失败:{self._error_caches}"
        )

    def __check_self_tracker(self, tracker_list):
        """校验种子的tracker"""
        if not tracker_list:
            return False
        domain_array = self._seed_domain.split("/")
        for tracker in tracker_list:
            if domain_array[2] in tracker:
                return True
        return False

    def __seed_torrents_path(self):
        """根据文件路径辅种"""
        if not self._seed_path:
            return
        if not os.path.exists(self._seed_path):
            return
        logger.debug(f"根据文件路径 开始")
        for root, dirs, files in os.walk(self._seed_path):
            for dir_name in dirs:
                self.__seed_torrent(None, dir_name, root, None, True)

        logger.debug(f"根据文件路径  完毕")

    def __seed_torrent(self, downloader, name, path, track_list, check_file=False):
        """根据名称和路径辅种"""
        try:
            can_seed = False
            for suff in self._torr_suff:
                if name.endswith(suff):
                    can_seed = True
                    break
            if not can_seed:
                return

            if self.__check_self_tracker(track_list):
                return

            seed_path = path
            size, file_count = self.__get_directory_info(path)
            name_key = f"{name}_{file_count}"
            if name_key in self._torrent_data:
                torrent_info = self._torrent_data[name_key]
                if check_file:
                    file_count_match = file_count == torrent_info["file_count"]
                    size_tolerance = max(torrent_info["size"] * 0.01, 1024 * 1024)
                    size_match = abs(size - torrent_info["size"]) <= size_tolerance
                    if not file_count_match or not size_match:
                        logger.info(
                            f"文件匹配失败 {name} file_count_match:{file_count_match} size_match:{size_match}"
                        )
                        return
                    if self._seed_path_replace:
                        split_array = self._seed_path_replace.split("|")
                        seed_path = seed_path.replace(split_array[0], split_array[1])
                self.__seed_red_torrents_by_downloader(
                    downloader, torrent_info["torrent_id"], seed_path
                )
                time.sleep(self._seed_delay)
            else:
                logger.info(f"__seed_torrent 未匹配到种子：{seed_path}/{name}")
        except Exception as e:
            logger.error(f"__seed_torrent 错误 {seed_path} {name} {str(e)}")

    def __seed_red_torrents_by_downloader(self, downloader, id, path):
        """根据种子id辅种"""
        if id in self._success_caches:
            return
        down_inst = None
        if downloader:
            inst = self._downloader_helper.get_service(name=downloader)
            down_inst = inst.instance
        else:
            inst = self._downloader_helper.get_service(name=self._default_downloader)
            down_inst = inst.instance
        torr_url = (
            f"{self._seed_domain}/download.php?id={id}&passkey={self._seed_passkey}"
        )
        download_id = down_inst.add_torrent(
            content=torr_url, download_dir=path, is_paused=True
        )
        if not download_id:
            self._error_caches.append(f"{id},{path}")
            logger.info(f"种子推送失败 下载器：{downloader} 种子{id}")
        else:
            self._seed_count = self._seed_count + 1
            self._success_caches.add(id)
            logger.info(f"种子推送成功 下载器：{downloader} 种子{id}")

    def __load_torrent_data(self):
        """加载种子数据"""

        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(
            current_dir, f"torrent_data_{self._match_torrent_id_col}.csv"
        )
        # 检查文件是否存在
        if not os.path.exists(data_path):
            logger.error(f"数据文件不存在: {data_path}")
            return

        logger.info(f"正在加载数据文件: {data_path}")

        # 重新打开文件进行正式读取
        with open(data_path, "r", encoding="utf-8") as f:
            # 尝试不同的分隔符
            reader = csv.DictReader(f, delimiter=",")
            # 读取数据
            row_count = 0
            for row in reader:
                try:
                    # 使用原始列名获取数据
                    name = row["name"].strip()
                    if not name:  # 跳过空行
                        continue

                    # 处理可能为空的字段
                    size_str = row["size"].strip()
                    file_count_str = row["file_count"].strip()
                    torrent_id_str = row["torrent_id"].strip()

                    self._torrent_data[f"{name}_{file_count_str}"] = {
                        "size": int(size_str) if size_str else 0,
                        "file_count": int(file_count_str) if file_count_str else 0,
                        "torrent_id": torrent_id_str if torrent_id_str else "",
                    }
                    row_count += 1

                except Exception as e:
                    logger.warning(f"处理第{row_count+2}行数据时出错: {row} {e}")

            logger.info(f"成功加载 {len(self._torrent_data)} 条种子数据")

    def __get_directory_info(self, dir_path: Path) -> Tuple[int, int]:
        """
        获取目录大小和文件数量

        Args:
            dir_path: 目录路径

        Returns:
            (目录大小(字节), 文件数量)
        """
        total_size = 0
        file_count = 0

        try:
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                        file_count += 1
                    except (OSError, IOError):
                        continue
        except Exception as e:
            logger.warning(f"扫描目录 {dir_path} 时出错: {e}")

        return total_size, file_count

    def __init_success_caches(self):
        logger.info("初始化辅种缓存")
        try:
            qb_downloaders = self._downloader_helper.get_services(
                type_filter="qbittorrent"
            )
            for downloader in qb_downloaders:
                inst = self._downloader_helper.get_service(name=downloader)
                inst_config = inst.config.config
                client = QbittorrentClient(
                    host=inst_config.get("host"),
                    username=inst_config.get("username"),
                    password=inst_config.get("password"),
                )
                torrs = client.torrents_info()
                for torrent in torrs:
                    self.__check_add_success_caches(
                        [torrent.get("tracker")], torrent.properties.get("comment")
                    )
        except Exception as e:
            logger.error(f"qb 初始化辅种缓存 失败 {str(e)}")

        try:
            tr_downloaders = self._downloader_helper.get_services(
                type_filter="transmission"
            )
            for downloader in tr_downloaders:
                inst = self._downloader_helper.get_service(name=downloader)
                inst_config = inst.config.config
                host_config = inst_config.get("host")
                parsed_url = urlparse(host_config)
                client = TransmissionClient(
                    host=parsed_url.hostname,
                    port=parsed_url.port,
                    username=inst_config.get("username"),
                    password=inst_config.get("password"),
                )
                list = client.get_torrents()
                for tr in list:
                    self.__check_add_success_caches(tr.tracker_list, tr.comment)
        except Exception as e:
            logger.error(f"tr 初始化辅种缓存 失败 {str(e)}")

        logger.info(f"初始化辅种缓存完毕 共缓存{len(self._success_caches)} 条数据")

    def __check_add_success_caches(self, tracker_list, comment):
        try:
            if not tracker_list:
                return
            if not comment:
                return
            if "id=" not in comment:
                return
            if self._seed_domain in comment or self.__check_self_tracker(tracker_list):
                id = comment.split("id=")[1]
                self._success_caches.add(id)
        except Exception as e:
            logger.error(f"检查缓失败 {comment} {str(e)}")
