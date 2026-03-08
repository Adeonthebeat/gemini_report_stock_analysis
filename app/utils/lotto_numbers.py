import random


def generate_lotto_numbers_with_weights():
    # 1. 1~45번까지의 역대 당첨 횟수 설정 (최근 통계 기반 예시)
    # 기본 당첨 횟수를 평균치인 150회 정도로 가정하고, 특정 번호의 빈도를 조절합니다.
    frequencies = {i: 150 for i in range(1, 46)}

    # 실제 역대 최다/최소 출현 번호 통계 일부 반영
    frequencies[43] = 192  # 역대 최다 출현 번호
    frequencies[34] = 190
    frequencies[12] = 189
    frequencies[27] = 187
    frequencies[9] = 140  # 역대 최소 출현 번호

    population = list(frequencies.keys())
    weights = list(frequencies.values())

    # 2. 가중치를 반영하여 중복 없이 6개 번호 추출
    result = []

    for _ in range(6):
        # 가중치(당첨 횟수)를 기반으로 번호 1개 뽑기
        chosen_number = random.choices(population, weights=weights, k=1)[0]
        result.append(chosen_number)

        # 로또는 중복된 번호가 나올 수 없으므로, 뽑힌 번호는 목록에서 제거
        index = population.index(chosen_number)
        population.pop(index)
        weights.pop(index)

    # 3. 뽑힌 번호를 보기 좋게 오름차순으로 정렬하여 반환
    return sorted(result)


# 추출기 실행
if __name__ == "__main__":
    print("🎲 통계 기반 로또 번호 추출기 🎲")
    for i in range(1):  # 5게임 연속 추출
        lotto_numbers = generate_lotto_numbers_with_weights()
        print(f"{i + 1}게임: {lotto_numbers}")