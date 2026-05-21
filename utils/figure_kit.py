class FigureColor:
    # 基础色系
    RED = "rgba(248, 113, 113, 1)"  # red-400
    GREEN = "rgba(74, 222, 128, 1)"  # green-400
    BLUE = "rgba(100, 149, 237, 1)"  # cornflower blue
    WHITE = "rgba(255, 255, 255, 1)"  # 白色
    GRAY = "rgba(209, 213, 219, 1)"  # gray-300
    YELLOW = "rgba(250, 204, 21, 1)"  # yellow-400

    # 常见色
    ORANGE = "rgba(251, 146, 60, 1)"  # orange-400
    PURPLE = "rgba(120, 81, 169, 1)"  # purple
    PINK = "rgba(244, 114, 182, 1)"  # pink-400
    BROWN = "rgba(168, 162, 158, 1)"  # stone-400 近似棕色
    BLACK = "rgba(0, 0, 0, 1)"  # 纯黑色

    # 数字媒体常用色
    CYAN = "rgba(34, 211, 238, 1)"  # cyan-400
    MAGENTA = "rgba(232, 121, 249, 1)"  # fuchsia-400 近似品红

    # 自然色系
    LIME = "rgba(163, 230, 53, 1)"  # lime-400
    TEAL = "rgba(45, 212, 191, 1)"  # teal-400
    NAVY = "rgba(30, 64, 175, 1)"  # blue-800 近似深蓝
    OLIVE = "rgba(168, 162, 158, 1)"  # stone-400 近似橄榄
    MAROON = "rgba(190, 24, 93, 1)"  # pink-700 近似栗色

    # 金属色
    SILVER = "rgba(228, 228, 231, 1)"  # zinc-200 近似银色
    GOLD = "rgba(251, 191, 36, 1)"  # amber-400 近似金色

    # 特殊色
    INDIGO = "rgba(129, 140, 248, 1)"  # indigo-400
    TURQUOISE = "rgba(45, 212, 191, 1)"  # teal-400 近似绿松石
    VIOLET = "rgba(167, 139, 250, 1)"  # violet-400
    CORAL = "rgba(251, 146, 60, 1)"  # orange-400 近似珊瑚
    SALMON = "rgba(252, 165, 165, 1)"  # red-300 近似鲑鱼色
    SKY_BLUE = "rgba(56, 189, 248, 1)"  # sky-400
    LAVENDER = "rgba(230, 230, 250, 1)"  # 非常浅的蓝紫，近白
    PERIWINKLE = "rgba(204, 204, 255, 1)"  # 近似紫罗兰色
    THISTLE = "rgba(216, 191, 216, 1)"  # 柔紫中灰调，亮度适中
    WISTERIA = "rgba(201, 160, 220, 1)"  # 偏灰紫色
    PALE_VIOLET = "rgba(219, 200, 240, 1)"  # 极浅紫

    # plotly默认颜色序列的自定义映射
    # plotly默认: ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']

    PLOTLY_DEFAULT_COLORWAY = [
        # "rgba(59, 130, 246, 1)",    # blue-500 #00CC96  近似
        PURPLE,
        WISTERIA,
        GREEN,  # #00CC96  近似 green-400
        CYAN,  # #19D3F3  近似 cyan-400
        THISTLE,
        RED,  # #EF553B  近似 red-400
        ORANGE,  # #FFA15A  近似 orange-400
        PINK,  # #FF6692  近似 pink-400
        LIME,  # #B6E880  近似 lime-400
        GOLD,  # #FECB52  近似 amber-400（金色）
    ]
