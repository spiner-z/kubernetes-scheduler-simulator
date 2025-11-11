# Plot

## Font
可能会出现中文字体无法显示的问题。

1. 查看全部字体
```bash
fc-list
```

2. 查看全部中文字体
```bash
fc-list :lang=zh
```

3. 如果没有中文字体，安装常见的中文包
```bash
# ubuntu
sudo apt-get update
sudo apt-get install ttf-wqy-microhei

# 安装文泉驿正黑（更粗，适合标题）
sudo apt-get install ttf-wqy-zenhei

# 安装思源黑体（开源通用字体，支持多语言）
sudo apt-get install fonts-noto-cjk
```

4. 清除 Matplotlib 字体缓存
```bash
# 查看Matplotlib缓存路径（在Python中执行）
import matplotlib
print(matplotlib.get_cachedir())
# 输出类似：/home/用户名/.cache/matplotlib

# 终端中删除缓存
rm -rf /home/用户名/.cache/matplotlib
```

