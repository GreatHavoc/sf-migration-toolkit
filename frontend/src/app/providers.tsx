"use client";

import { App, ConfigProvider } from "antd";
import type { PropsWithChildren } from "react";

const themeConfig = {
  token: {
    colorPrimary: "#1f8a70",
    colorInfo: "#2f7062",
    colorSuccess: "#2e7d5f",
    colorWarning: "#b66a1f",
    colorError: "#b2432f",
    colorTextBase: "#17222b",
    colorBgBase: "#f3f6f2",
    borderRadius: 14,
    fontFamily: 'var(--font-sora), "Trebuchet MS", "Segoe UI", sans-serif',
  },
  components: {
    Card: {
      headerFontSize: 16,
      bodyPadding: 20,
    },
    Tabs: {
      itemColor: "#32505c",
      itemSelectedColor: "#1f8a70",
      inkBarColor: "#1f8a70",
    },
    Input: {
      activeBorderColor: "#1f8a70",
      hoverBorderColor: "#4a8f7c",
    },
    Select: {
      activeBorderColor: "#1f8a70",
      hoverBorderColor: "#4a8f7c",
    },
    Button: {
      primaryShadow: "0 8px 22px rgba(31, 138, 112, 0.2)",
    },
  },
} as const;

export default function Providers({ children }: PropsWithChildren) {
  return (
    <ConfigProvider theme={themeConfig}>
      <App>{children}</App>
    </ConfigProvider>
  );
}
