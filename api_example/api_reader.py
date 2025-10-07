import pandas as pd
import requests
from tqdm import tqdm
import time


# ищем ID популярных аниме
def search_for_anime_id(anime_names):
    """
    Функция ищет ID аниме по названиям
    Возвращает список словарей с найденными аниме
    """
    search_url = "https://api.jikan.moe/v4/anime"
    anime_list = []
    for name in anime_names:
        params = {"q": name, "limit": 1}
        response = requests.get(search_url, params=params)

        if response.status_code == 200:
            answer = response.json()

            if answer["data"]:
                anime_data = answer["data"][0]
                anime_list.append(
                    {
                        "name": name,
                        "id": anime_data["mal_id"],
                        "title": anime_data["title"],
                    }
                )
                print(f"Найдено: {name} -> ID: {anime_data['mal_id']}")
            else:
                print(f"Не найдено: {name}")
        else:
            print(f"Ошибка для {name}: {response.status_code}")
    return anime_list


anime_names = ["Chainsaw Man", "Attack on Titan", "Jujutsu Kaisen"]
anime_list = search_for_anime_id(anime_names)

# создаем DataFrame
if anime_list:
    df = pd.DataFrame(anime_list)
    print("\n📊 Итоговая таблица с аниме ID:")
else:
    print("Не удалось найти ни одного аниме")

# ждем 60 секунд между запросами - ограниченние API
for i in tqdm(range(60)):
    time.sleep(1)


# ищем полную инофрмациб об аниме зная его ID
def get_anime_details(anime_list):
    """
    Функция получает детальную информацию об аниме по списку ID
    Возвращает список с детальной информацией
    """
    detailed_anime_list = []
    for anime in anime_list:
        anime_id = anime["id"]
        detail_url = f"https://api.jikan.moe/v4/anime/{anime_id}/full"
        detail_response = requests.get(detail_url)

        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            detailed_anime_list.append(
                {
                    "id": anime_id,
                    "title": anime["title"],
                    "episodes": detail_data["data"].get("episodes"),
                    "score": detail_data["data"].get("score"),
                    "status": detail_data["data"].get("status"),
                    "synopsis": detail_data["data"].get("synopsis"),
                    "year": detail_data["data"].get("year"),
                    "season": detail_data["data"].get("season"),
                }
            )
        else:
            print(f"Ошибка для ID {anime_id}: {detail_response.status_code}")
    return detailed_anime_list


detailed_anime_list = get_anime_details(anime_list)

from IPython.display import display

if detailed_anime_list:
    detailed_df = pd.DataFrame(detailed_anime_list)
    print("\n📊 Детальная информация об аниме:")
    display(detailed_df)
