import type { Metadata } from "next";
import { IBM_Plex_Mono, Sora } from "next/font/google";
import { AntdRegistry } from "@ant-design/nextjs-registry";
import Providers from "./providers";
import "./globals.css";

const sora = Sora({
  variable: "--font-sora",
  weight: ["400", "500", "600", "700"],
  subsets: ["latin"],
});

const ibmPlexMono = IBM_Plex_Mono({
  variable: "--font-ibm-plex-mono",
  weight: ["400", "500", "600"],
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Snowflake Migrate Control Center",
  description: "Standalone migration UI for Snowflake-to-Snowflake workflows",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${sora.variable} ${ibmPlexMono.variable} h-full antialiased`}
    >
      <body className="min-h-full">
        <AntdRegistry>
          <Providers>{children}</Providers>
        </AntdRegistry>
      </body>
    </html>
  );
}
