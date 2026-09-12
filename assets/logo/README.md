# RushWind · 锐风 — Brand Assets

## 概念

主标志是一个字母 **R** 单标：Rush 谐音 Rust，R 的斜腿像一阵风一样逐渐收细、甩出，
末端化作一道分离的风痕——「疾驰的速度」与「Wind（风）」的意象合一。
左侧两道短促的进风口暗示风从何处来。

## 文件

| 文件 | 用途 |
|:---|:---|
| `rushwind-icon.svg` | 主徽标：深色圆角徽章，用于头像 / favicon / 应用图标 |
| `rushwind-icon-light.svg` | 仅图形（透明底），用于浅色背景 |
| `rushwind-icon-mono.svg` | 单色版（深墨色），用于印刷 / 水印 / 深色文字场景 |
| `rushwind-icon-mono-inverse.svg` | 单色反白版，用于深色背景 |
| `rushwind-lockup-dark.svg` | 横版组合（深色文字），用于浅色背景页头 |
| `rushwind-lockup-light.svg` | 横版组合（反白文字），用于深色背景页头 |
| `favicon.svg` | 现代浏览器 favicon（= 主徽章） |
| `favicon.ico` | 传统 favicon（内含 16/32/48 三档位图） |
| `png/icon-512.png` `png/icon-192.png` | PWA / 通用应用图标（圆角徽章） |
| `png/apple-touch-icon.png` | iOS 桌面图标（180×180 全出血方形，系统自行圆角） |
| `png/favicon-{48,32,16}.png` | 分档位图 favicon |
| `preview.html` | 浏览器打开可预览全部变体与缩放测试 |
| `preview.png` | 预览图快照 |

## 前端 / App Icon 接入

把 `favicon.svg`、`favicon.ico`、`png/` 拷入前端静态目录后：

```html
<link rel="icon" href="/favicon.ico" sizes="48x48">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="manifest" href="/site.webmanifest">
```

`site.webmanifest` 示例：

```json
{
  "name": "RushWind",
  "icons": [
    { "src": "/icon-192.png", "sizes": "192x192", "type": "image/png" },
    { "src": "/icon-512.png", "sizes": "512x512", "type": "image/png" },
    { "src": "/apple-touch-icon.png", "sizes": "180x180", "type": "image/png", "purpose": "maskable" }
  ]
}
```

PNG 均由 `rushwind-icon.svg` 栅格化导出；修改图形后需重新导出同名文件。

## 色板

| 色值 | 名称 |
|:---|:---|
| `#2DD4BF` | Wind Teal（风·青） |
| `#38BDF8` | Wind Sky（风·天） |
| `#818CF8` | Wind Indigo（风·靛） |
| `#16233F → #0A0F1E` | 徽章底（Deep Navy → Ink） |
| `#0F172A` / `#F8FAFC` | 墨 / 雪（文字用色） |

图形渐变方向固定为**左下 → 右上**（青 → 天 → 靛），与「风掠过」的方向一致。

## 使用规则

- 图形四周保留不小于图形高度 12% 的净空。
- 最小可用尺寸：徽章 16px（favicon 已含此档）。
- 浅色背景上的独立图形请用 `rushwind-icon-light.svg`（更深的青→靛渐变），不要直接叠主徽章渐变。
- iOS 图标必须用 `apple-touch-icon.png`（全出血方形），不要提交透明圆角图。
- 不要拉伸、旋转、改变渐变方向，也不要给图形加描边或投影。
