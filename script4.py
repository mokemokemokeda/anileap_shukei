# チャット収集ループ（最大4時間）
start_time = time.time()
MAX_DURATION = 4 * 60 * 60  # 4時間 = 14400秒
print_count = 0

while time.time() - start_time < MAX_DURATION:
    try:
        new_data = []
        for c in chat.get().items:
            new_data.append([c.datetime, c.author.name, c.message])
            # ログ出力を控えめに（100件に1回だけ表示）
            if print_count % 100 == 0:
                print(f"[{c.datetime}] {c.author.name}: {c.message}")
            print_count += 1

        if new_data:
            new_df = pd.DataFrame(new_data, columns=["timestamp", "username", "message"])
            history_df = pd.concat([history_df, new_df], ignore_index=True)

            # アップロード処理（略：元のコードと同じ）
            # ...（drive_service.files().update/create）...

    except Exception as e:
        print(f"チャット取得エラー: {e}")

    time.sleep(5)
