import os


def generate_links(directory):
    links = []
    base_url = "https://gwas.mrcieu.ac.uk/files/"
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if "	" in line:
                        line = line.split("	")[0]
                    link = base_url + line + "/" + line + ".vcf.gz"
                    links.append(link)
    return links


def save_links_to_file(links, output_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        for link in links:
            file.write(link + '\n')


if __name__ == "__main__":
    target_directory = r"D:\document\bioInfo\alps-eye\outcome"
    output_file_path = r"D:\document\bioInfo\alps-eye\generated_links.txt"

    all_links = generate_links(target_directory)
    save_links_to_file(all_links, output_file_path)
    print(f"所有链接已保存到 {output_file_path} 文件中。")