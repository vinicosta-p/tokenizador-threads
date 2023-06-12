#include <iostream>
#include <fstream>
#include <chrono>
#include <mutex> 
#include <thread>
#include <map>
#include <sstream>  // for string streams
#include <string>  // for string

using namespace std;
fstream DATASET_CSV, DATASET_FINAL;
// COLUNAS COM DADOS CARTEGORICAS: 2, 3, 4, 6, 7, 8, 9, 17, 18, 20, 23
// cada var representa o nome de uma coluna
fstream cdtup,	berco,	portoatracacao,	mes, tipooperacao,	tiponavegacaoatracacao,	terminal,
nacionalidadearmador, origem, destino, naturezacarga, sentido;

// BUFFER 
const int BUFFER_SIZE = 101;
const int BUFFER_COL = 26;
string buffer_de_escrita[BUFFER_SIZE][BUFFER_COL];
string palavra_excedente;

mutex mtx_buffer; // para o buffer 
mutex mtx_final; // para o arquivo final
mutex mtx_conditional_escrita;
mutex mtx_leitura_do_buffer;
//Definem o fim das interações
int interacoes_internas = 0;
int interacoes_externas = 0;
bool EVENT_BREAK_LINE = false;
//Define se a linha já pode ser escrita
const int QNTD_DE_THREADS = 6;
bool flag_escreva[BUFFER_SIZE][QNTD_DE_THREADS] = {false};

bool inicializou_a_primeira_linha = false; // variavel apenas para ler uma unica vez
void leitura_do_arquivo(){
    if(inicializou_a_primeira_linha == false){
        mtx_final.lock();
        DATASET_CSV.open("dataset_00_1000_sem_virg.csv", ios::in);
        DATASET_FINAL.open("dataset_final.csv",ios::out);
        // escreve a primeira linha no csv
        string primeira_linha;
        getline(DATASET_CSV, primeira_linha);
        DATASET_FINAL << primeira_linha.c_str() << endl;
        mtx_final.unlock();
        
        inicializou_a_primeira_linha = true;
    }
    string linha;
    char delim = ',';
    /*
    A intenção no código pe que ele tenha a seguinte etapa

    uma thread lerá o arquivo inicial e escreverá em um buffer,
    ao fim da escrita do buffer a thread deve ficar "fechada" enquanto 
    as outras são liberadas para tratar o buffer. Houveram muitas tentativas
    com mutex, whiles, até hibernate mas nenhuma deu certo.

    esta thread tbm define o fim das outras
    quando ela acaba ela avisa através de uma variavel EVENT_BREAK_LINE
    e as threads começam a verificar se já é para acbar
    */
    if (DATASET_CSV.is_open()){
        int i = 0;
        int j = 0;
        int teste_de_interacao = 0;
        while(getline(DATASET_CSV, linha, delim)){
            if(i == BUFFER_SIZE){
                interacoes_externas++;
                std::this_thread::sleep_for(std::chrono::milliseconds(3));
                i = 0;
                
            }
                
           
            mtx_buffer.lock();
            
            if(j == 25){
                buffer_de_escrita[i][j] = linha.substr(0, linha.find("\n"));
                if (i == BUFFER_SIZE-1){
                    palavra_excedente = linha.substr(linha.find('\n'), linha.length());
                } else {
                    buffer_de_escrita[i+1][0] = linha.substr(linha.find('\n'), linha.length());
                }
            } else {
                buffer_de_escrita[i][j] = linha;
            }
            mtx_leitura_do_buffer.lock();
            flag_escreva[i][0] = true;
            mtx_leitura_do_buffer.unlock();
            mtx_buffer.unlock();
            
            linha.clear();
            j++;
            if(j == 26){
                i++;
                j = 1;
            }
            
        }
        interacoes_internas = i;
        EVENT_BREAK_LINE = true;
        DATASET_CSV.close();
    }
    
}
void escrita_do_arquivo(){
    int i = 0;
    int idx_interacoes = 0;
    while(true){

        while(i != BUFFER_SIZE){

            if(EVENT_BREAK_LINE == true){ 
                if(idx_interacoes >= interacoes_externas && interacoes_internas-1 <= i){
                    break;
                }
            }
            //flag que verifica se os processos já acabaram na linha i se sim ele escreve no arquivo
            if(flag_escreva[i][0] == true && flag_escreva[i][1] == true && flag_escreva[i][2] == true && 
                flag_escreva[i][3] == true && flag_escreva[i][4] == true && flag_escreva[i][5] == true){
                mtx_final.lock();
                mtx_buffer.lock();
                for(int j = 0; j < BUFFER_COL; j++){
                    DATASET_FINAL << buffer_de_escrita[i][j].c_str() << ","; 
                }
                mtx_leitura_do_buffer.lock();
                for(int idx_flag = 0; idx_flag < QNTD_DE_THREADS; idx_flag++){
                    flag_escreva[i][idx_flag] = false;
                   // cout << "f"<< idx_flag << endl;
                }
                mtx_leitura_do_buffer.unlock();
                mtx_buffer.unlock();
                mtx_final.unlock();
                if(i == 100){
                    std::this_thread::sleep_for(std::chrono::milliseconds(3));
                }
                i++;
            }       
        }

    
        //cout << idx_interacoes << " " << i << endl;
        if(EVENT_BREAK_LINE == true){ 
            if(idx_interacoes >= interacoes_externas &&  i >= interacoes_internas-1){
            //cout << "FIM: " << interacoes_externas << " " << interacoes_internas << endl;
            break;
            }
        }
        idx_interacoes++;
        i = 0;
    }
}

void escreve_id(int coluna, int id_da_thread, int col_2, int id_thread_2, bool dois_valores){
        
        int idx_mudança = coluna;
        int idx_interacoes = 0;
        fstream arquivo;
        bool existe_no_dic = false;
        int i = 0;
        int id = 1;
        int id_2 = 1;
        map<string, int> dicionario;
        map<string, int> dicionario2;
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
        while (true)
        {   
       
        while (i != BUFFER_SIZE) {
                    
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
                    
            if(EVENT_BREAK_LINE == true){ 
                if(idx_interacoes >= interacoes_externas &&  i >= interacoes_internas-1 ){
                        break;
                    }
            }
                    
                mtx_buffer.lock();
                string pesquisa_no_dicionario = buffer_de_escrita[i][idx_mudança];
                pesquisa_no_dicionario.append(",");
                if(dicionario[pesquisa_no_dicionario] == false || dicionario.empty()){
                    dicionario[pesquisa_no_dicionario] = id;
                    id++;
                }
                // tranforma o int
                ostringstream aux;
                aux << dicionario[pesquisa_no_dicionario];
                string id_char = aux.str();

                    
                buffer_de_escrita[i][idx_mudança] = id_char;
                   
                //repete o que foi feito                   
                if(dois_valores == true){
                        
                    string pesquisa_no_dicionario_2 = buffer_de_escrita[i][col_2];
                    pesquisa_no_dicionario_2.append(",");
                    if(dicionario2[pesquisa_no_dicionario_2] == false || dicionario2.empty()){
                        dicionario2[pesquisa_no_dicionario_2] = id_2;
                        id_2++;
                    }
                    ostringstream aux_2;
                    aux_2 << dicionario2[pesquisa_no_dicionario_2];
                    string id_char_2 = aux_2.str();
                    buffer_de_escrita[i][col_2] = id_char_2;

                }
                    
                    mtx_leitura_do_buffer.lock();
                    flag_escreva[i][id_da_thread] = true;
                    mtx_leitura_do_buffer.unlock();
                    
                    mtx_buffer.unlock();
                    
                    i++;
                    
                    pesquisa_no_dicionario.clear(); 

                    }
                
        
                    if(EVENT_BREAK_LINE == true){ 
                        if(idx_interacoes >= interacoes_externas &&  i >= interacoes_internas-1){
                            break;
                        }
                    }
              

            
            idx_interacoes++;
            i = 0;
        }       
}

int main(){
    auto start = std::chrono::steady_clock::now();
    
    thread TH1(leitura_do_arquivo);
    thread TH2(escreve_id, 1, 1, 17, 7, true);
    thread TH3(escreve_id, 2, 2, 18, 8, true);
    thread TH4(escreve_id, 3, 3, 20, 9, true);
    thread TH5(escreve_id, 5, 4, 23, 10, true);
    thread TH6(escreve_id, 8, 5, 6, 11, true);
    thread TH7(escreve_id, 7, 6, 0, 0, false);
    thread TH8(escrita_do_arquivo);

    TH1.join();
    TH2.join();
    TH3.join();
    TH4.join();
    TH5.join();
    TH6.join();
    TH7.join();
    TH8.join();

    auto end = std::chrono::steady_clock::now();
    std::cout << "Tempo: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    return 0;
}