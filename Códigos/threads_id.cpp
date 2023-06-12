#include <iostream>
#include <fstream>
#include <chrono>
#include <mutex> 
#include <thread>
#include <map>
#include <sstream>  // for string streams
#include <string>  // for string

using namespace std;

    mutex bloqueio;
    bool last = false;

    ifstream DATASET_CSV;
    // COLUNAS COM DADOS CARTEGORICAS: 2, 3, 4, 6, 7, 8, 9, 17, 18, 20, 23
    // cada var representa o nome de uma coluna
    // possível melhoria: trabalhar com vetores de ifstream para tornar dinamico
    // e verificar quais colunas são categóricas
    fstream cdtup,	berco,	portoatracacao,	mes, tipooperacao,	tiponavegacaoatracacao,	terminal,
    nacionalidadearmador, origem, destino, naturezacarga, sentido;

void principal(){
    bloqueio.lock();
    char delim = ',';
    DATASET_CSV.open("dataset_00_1000_sem_virg.csv", ios::in);
 

    //Inicialização das colunas
    cdtup.open("cdtup.csv", ios::out);	
    berco.open("berco.csv", ios::out);	
    portoatracacao.open("portoatracacao.csv", ios::out);
    mes.open("mes.csv", ios::out);
    tipooperacao.open("tipooperacao.csv", ios::out);
    tiponavegacaoatracacao.open("tiponavegacaoatracacao.csv", ios::out);
    terminal.open("terminal.csv", ios::out);
    origem.open("origem.csv", ios::out);
    destino.open("destino.csv", ios::out);
    naturezacarga.open("naturezacarga.csv", ios::out);
    sentido.open("sentido.csv", ios::out);




    // FUNC: Varrer o arquivo para criar um arquivo csv para cada coluna cartegórica
    string linha;
    int coluna = 0;
    int count = 0;
    // Não utilizei threads para a varredura e escrita por causa da seção crítica do arquivo do DATASET
    // porém pode haver uma melhoria

    /* melhorias para o trecho:
     * verificar quais colunas tem que ser varridas e utilizar o continue para nao ter verificação
     * Utilizar tratamento de seção crítica para a leitura do DataSet 
     * Parlelizar a escrita trocando o WHILE por um FOR e usando o Parallel for colapse(2)(as threads 
        serão divididas por colunas)
     * REQUISITO PARA NOVA VARREDURA: Existir um contador de linha x coluna para utilizar de parametro para o FOR

    */
    if (DATASET_CSV.is_open()){
        while(getline(DATASET_CSV, linha, delim)){
        coluna++;
        // fim da coluna
        if (coluna == 25){
            coluna = 0;
            continue;
        }

        //if(coluna != 2, 3, 4, 6, 7, 8, 9, 17, 18, 20, 23)

        switch (coluna){
        case 2:
            cdtup << linha << endl;
            break;
        case 3:
            berco << linha << endl;
            break;
        case 4:
            portoatracacao << linha << endl;
            break;
        case 6:
            mes << linha << endl;
            break;
        case 7:
            tipooperacao << linha << endl;
            break;    
        case 8:                     
            tiponavegacaoatracacao << linha << endl;
            break;
        case 9:
            terminal << linha << endl;
            break;
        case 18:
            origem << linha << endl;
            break;
        case 19:
            destino << linha << endl;
            break;
        case 21:
            naturezacarga << linha << endl;
            break;
        case 24:
            sentido << linha << endl;
            break;
        default:
            break;
        }
        }

    }
    DATASET_CSV.close();
    bloqueio.unlock();
}
void escreve_id(fstream &arquivo);

void principal_final(){
     while (true)
    {
        if(bloqueio.try_lock() and last){
            break;
        }
    }
    cdtup.open("cdtup_id.csv", ios::in);	
    berco.open("berco_id.csv", ios::in);	
    portoatracacao.open("portoatracacao_id.csv", ios::in);
    mes.open("mes_id.csv", ios::in);
    tipooperacao.open("tipooperacao_id.csv", ios::in);
    tiponavegacaoatracacao.open("tiponavegacaoatracacao_id.csv", ios::in);
    terminal.open("terminal_id.csv", ios::in);
    origem.open("origem_id.csv", ios::in);
    destino.open("destino_id.csv", ios::in);
    naturezacarga.open("naturezacarga_id.csv", ios::in);
    sentido.open("sentido_id.csv", ios::in);

    ofstream DATASET_FINAL;
    DATASET_FINAL.open("dataset_com_ids.csv", ios::app);

    DATASET_CSV.open("dataset_00_1000_sem_virg.csv", ios::in);
    DATASET_CSV.seekg(ios::beg);
    char delim = ',';
    string linha;
    string dado;
    bool primeira_leitura = true;
    int coluna = 0;
    getline(cdtup, dado);
 
    getline(berco, dado);
      
    getline(portoatracacao, dado);
    
    getline(mes, dado);
       
    getline(tipooperacao, dado);
      
    getline(tiponavegacaoatracacao, dado);

    getline(terminal, dado);

    getline(origem, dado);
 
    getline(destino, dado);

    getline(naturezacarga, dado);

    getline(sentido, dado);
    linha.clear();
    dado.clear();
    if (DATASET_CSV.is_open()){
        while(getline(DATASET_CSV, linha, delim)){
        coluna++;
        // fim da coluna
        if (coluna == 25){
            coluna = 0;
            DATASET_FINAL << linha.append(",");
            dado.clear();
            linha.clear();
            primeira_leitura = false;
            continue;
        }

        if(primeira_leitura){
            DATASET_FINAL << linha.append(",");
            continue;
        }

        //if(coluna != 2, 3, 4, 6, 7, 8, 9, 17, 18, 20, 23)

        switch (coluna){
        case 2:
            getline(cdtup, dado);
            DATASET_FINAL << dado;
            break;
        case 3:
            getline(berco, dado);
            DATASET_FINAL << dado;
            break;
        case 4:
            getline(portoatracacao, dado);
            DATASET_FINAL << dado;
            break;
        case 6:
            getline(mes, dado);
            DATASET_FINAL << dado;
            break;
        case 7:
            getline(tipooperacao, dado);
            DATASET_FINAL << dado;
            break;    
        case 8:     
            getline(tiponavegacaoatracacao, dado);
            DATASET_FINAL << dado;                
            break;
        case 9:
            getline(terminal, dado);
            DATASET_FINAL << dado;
            break;
        case 18:
            getline(origem, dado);
            DATASET_FINAL << dado;
            break;
        case 19:
            getline(destino, dado);
            DATASET_FINAL << dado;
            break;
        case 21:
            getline(naturezacarga, dado);
            DATASET_FINAL << dado;
            break;
        case 24:
            getline(sentido, dado);
            DATASET_FINAL << dado;
            break;
        default:
            DATASET_FINAL << linha.append(",");
            break;
        dado.clear();
        linha.clear();
        }
        }
    }
    bloqueio.unlock();
    cdtup.close();
    berco.close();	
    portoatracacao.close();
    mes.close();
    tipooperacao.close();
    tiponavegacaoatracacao.close();
    terminal.close();
    origem.close();
    destino.close();
    naturezacarga.close();
    sentido.close();
    remove("cdtup_id.csv");
    remove("berco_id.csv");
    remove("portoatracacao_id.csv");
    remove("mes_id.csv");
    remove("tipooperacao_id.csv");
    remove("tiponavegacaoatracacao_id.csv");
    remove("terminal_id.csv");
    remove("origem_id.csv");
    remove("destino_id.csv");
    remove("naturezacarga_id.csv");
    remove("cdtup_id.csv");
    remove("sentido_id.csv");

}

void funcao_do_meio(){
    while (true)
    {
        if(bloqueio.try_lock()){
            break;
        }
    }
    cdtup.close();
    cdtup.open("cdtup.csv", ios::in);	
    escreve_id(cdtup);
    
    berco.close();
    berco.open("berco.csv", ios::in);	
    escreve_id(berco);
    
    portoatracacao.close();
    portoatracacao.open("portoatracacao.csv", ios::in);	
    escreve_id(portoatracacao);

    mes.close();
    mes.open("mes.csv", ios::in);
    escreve_id(mes);
    
    tipooperacao.close();
    tipooperacao.open("tipooperacao.csv", ios::in);
    escreve_id(tipooperacao);
    
    tiponavegacaoatracacao.close();
    tiponavegacaoatracacao.open("tiponavegacaoatracacao.csv", ios::in);
    escreve_id(tiponavegacaoatracacao);

    terminal.close();
    terminal.open("terminal.csv", ios::in);
    escreve_id(terminal);
    
    origem.close();
    origem.open("origem.csv", ios::in);   
    escreve_id(origem);

    destino.close();
    destino.open("destino.csv", ios::in);
    escreve_id(destino);
    
    naturezacarga.close();
    naturezacarga.open("naturezacarga.csv", ios::in);
    escreve_id(naturezacarga);
    
    sentido.close();
    sentido.open("sentido.csv", ios::in);    
    escreve_id(sentido);
    last = true;
    bloqueio.unlock();

}
int main() {
    auto start = std::chrono::steady_clock::now();
    thread th1(principal);
    thread th2(funcao_do_meio);
    thread th3(principal_final);
    th1.join();
    th2.join();
    th3.join();



    // FIM DA LEITURA E ESCRITA DE ARQUIVOS
    
    // FUNC 2: PARALELIZANDO A IDENTIFICAÇÃO DE ID E ESCRITA DE ARQUIVO
    
    /*
    thread th1(escreve_id,cdtup);
    th1.join();
    thread th2(escreve_id,berco);
    th2.join();
    thread th3(escreve_id,portoatracacao);
    th3.join();
    thread th4(escreve_id,mes);
    th4.join();
    thread th5(escreve_id,tipooperacao);
    th5.join();
    thread th6(escreve_id,tiponavegacaoatracacao);
    th6.join();
    */
    //FUNC 3: ESCREVENDO NO ARQUIVO
    
    
    auto end = std::chrono::steady_clock::now();
    std::cout << "Tempo       : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    return 0;
}

void escreve_id(std::fstream &arquivo){
    string linha;
    ofstream ID_Arquivo;
    map<string, int> dicionario;
    int id = 1; 
    bool primeira_interacao = true;
    int cursor_pos = 0;
    arquivo.seekg(ios::beg);
    string nome_arquivo_id;
    string nome_do_arquivo_atual;
    

    while (getline(arquivo, linha))
    {
            //Altera o nome do arquivo para a coluna que está sendo varrida
        if(primeira_interacao  == true){
        
            nome_do_arquivo_atual = linha;
            // função nomear arquivo novo aberto
            nome_arquivo_id = linha;
            nome_arquivo_id.append("_id.csv");
            ID_Arquivo.open(nome_arquivo_id, ios::in | ios::app);


            ID_Arquivo << linha << endl;
            
            primeira_interacao = false;
            continue;
        }

        

        //Cria e adiciona um dicionario para o arquivo
        if(dicionario[linha] == false || dicionario.empty()){
            dicionario[linha] = id;
            id++;
        }
        // será transformado em função
        // converte inteiro para string para utilizar na escrita
        ostringstream aux;
        aux << dicionario[linha];
        string id_char = aux.str();
        id_char.append(",");
    
        ID_Arquivo << id_char << endl;
       
    }
    // apaga arquivo de coluna;
    nome_do_arquivo_atual.append(".csv");
    arquivo.close();
    remove(nome_do_arquivo_atual.c_str());
}