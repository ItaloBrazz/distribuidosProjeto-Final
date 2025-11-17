import threading
import time
import queue
from dataclasses import dataclass
from typing import List
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd


os.makedirs("results", exist_ok=True)
os.makedirs("charts", exist_ok=True)
os.makedirs("reports", exist_ok=True)

@dataclass
class ProductionMetrics:
    """Métricas de produção e consumo"""
    total_produced: int = 0
    total_consumed: int = 0
    producer_wait_times: List[float] = None
    consumer_wait_times: List[float] = None
    buffer_states: List[int] = None
    
    def __post_init__(self):
        if self.producer_wait_times is None:
            self.producer_wait_times = []
        if self.consumer_wait_times is None:
            self.consumer_wait_times = []
        if self.buffer_states is None:
            self.buffer_states = []

class ProductionLine:
    """Sistema de controle da linha de produção"""
    
    def __init__(self, buffer_capacity: int, num_producers: int, 
                 num_consumers: int, total_timesteps: int):
        assert buffer_capacity >= 1000, "Capacidade do buffer deve ser >= 1000"
        assert num_producers >= 200, "Número de produtores deve ser >= 200"
        assert num_consumers >= num_producers + 10, f"Número de consumidores deve ser >= {num_producers + 10}"
        assert total_timesteps >= 1000000, "Número de timesteps deve ser >= 1000000"
        
        self.buffer_capacity = buffer_capacity
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.total_timesteps = total_timesteps
        
        self.buffer = queue.Queue(maxsize=buffer_capacity)
        
        # Semáforos
        self.empty_slots = threading.Semaphore(buffer_capacity)  
        self.filled_slots = threading.Semaphore(0) 
        
        self.mutex = threading.Lock()
        
        self.current_timestep = 0
        self.timestep_lock = threading.Lock()
        self.running = True
        
        self.metrics = ProductionMetrics()
        self.metrics_lock = threading.Lock()
        
        # Threads
        self.producer_threads: List[threading.Thread] = []
        self.consumer_threads: List[threading.Thread] = []
        
        self.start_time = None
        
    def producer(self, producer_id: int):
        """Thread produtora - adiciona itens ao buffer"""
        while self.running and self.current_timestep < self.total_timesteps:
            wait_start = time.time()
            
            self.empty_slots.acquire()
            
            if not self.running or self.current_timestep >= self.total_timesteps:
                self.empty_slots.release()
                break
            
            wait_time = time.time() - wait_start
            
            with self.mutex:
                if self.current_timestep < self.total_timesteps:
                    item = f"P{producer_id:04d}_T{self.current_timestep}"
                    self.buffer.put(item)
                    
                    with self.metrics_lock:
                        self.metrics.total_produced += 1
                        self.metrics.producer_wait_times.append(wait_time)
                        if len(self.metrics.buffer_states) < 10000:  
                            self.metrics.buffer_states.append(self.buffer.qsize())
            
            self.filled_slots.release()
    
    def consumer(self, consumer_id: int):
        """Thread consumidora - retira itens do buffer"""
        while self.running:
            wait_start = time.time()
            
            self.filled_slots.acquire()
            
            if not self.running:
                self.filled_slots.release()
                break
            
            wait_time = time.time() - wait_start
            
            with self.mutex:
                if not self.buffer.empty():
                    item = self.buffer.get()
                    
                    with self.metrics_lock:
                        self.metrics.total_consumed += 1
                        self.metrics.consumer_wait_times.append(wait_time)
            
            self.empty_slots.release()
    
    def timestep_controller(self):
        """Controla a progressão dos timesteps"""
        progress_step = self.total_timesteps // 10
        while self.current_timestep < self.total_timesteps:
            with self.timestep_lock:
                self.current_timestep += 1
                
            if self.current_timestep % progress_step == 0:
                progress = (self.current_timestep / self.total_timesteps) * 100
                print(f"Progresso: {progress:.1f}% - Produzidos: {self.metrics.total_produced} - Consumidos: {self.metrics.total_consumed}")
        
        time.sleep(1)
        self.running = False
        
        for _ in range(self.num_producers + self.num_consumers):
            self.empty_slots.release()
            self.filled_slots.release()
    
    def run(self):
        """Inicia a simulação da linha de produção"""
        print("="*60)
        print("INICIANDO SIMULAÇÃO DA LINHA DE PRODUÇÃO")
        print("="*60)
        print(f"Capacidade do Buffer: {self.buffer_capacity}")
        print(f"Número de Produtores: {self.num_producers}")
        print(f"Número de Consumidores: {self.num_consumers}")
        print(f"Total de Timesteps: {self.total_timesteps}")
        print("="*60)
        
        self.start_time = time.time()
        
        for i in range(self.num_producers):
            t = threading.Thread(target=self.producer, args=(i,), name=f"Producer-{i:04d}")
            self.producer_threads.append(t)
            t.start()
        
        for i in range(self.num_consumers):
            t = threading.Thread(target=self.consumer, args=(i,), name=f"Consumer-{i:04d}")
            self.consumer_threads.append(t)
            t.start()
        
        controller = threading.Thread(target=self.timestep_controller, name="TimestepController")
        controller.start()
        
        controller.join()
        
        for t in self.producer_threads:
            t.join()
        for t in self.consumer_threads:
            t.join()
        
        execution_time = time.time() - self.start_time
        
        # Relatório final
        self.print_report(execution_time)
        
        return self.metrics, execution_time
    
    def print_report(self, execution_time: float):
        """Imprime relatório final da simulação"""
        print("\n" + "="*60)
        print("RELATÓRIO FINAL DA SIMULAÇÃO")
        print("="*60)
        print(f"Tempo de Execução: {execution_time:.2f} segundos")
        print(f"Total de Itens Produzidos: {self.metrics.total_produced:,}")
        print(f"Total de Itens Consumidos: {self.metrics.total_consumed:,}")
        print(f"Itens Restantes no Buffer: {self.buffer.qsize()}")
        
        if self.metrics.producer_wait_times:
            avg_producer_wait = sum(self.metrics.producer_wait_times) / len(self.metrics.producer_wait_times)
            print(f"Tempo Médio de Espera (Produtores): {avg_producer_wait*1000:.4f} ms")
        
        if self.metrics.consumer_wait_times:
            avg_consumer_wait = sum(self.metrics.consumer_wait_times) / len(self.metrics.consumer_wait_times)
            print(f"Tempo Médio de Espera (Consumidores): {avg_consumer_wait*1000:.4f} ms")
        
        efficiency = (self.metrics.total_consumed / self.metrics.total_produced * 100) if self.metrics.total_produced > 0 else 0
        print(f"Eficiência do Sistema: {efficiency:.2f}%")
        print("="*60)

def run_experiment(buffer_capacity: int, num_producers: int, 
                   num_consumers: int, total_timesteps: int):
    """Executa um experimento com configuração específica"""
    print(f"\n🚀 Executando experimento...")
    print(f"   Buffer: {buffer_capacity}, Prod: {num_producers}, Cons: {num_consumers}, Steps: {total_timesteps}")
    
    production_line = ProductionLine(
        buffer_capacity=buffer_capacity,
        num_producers=num_producers,
        num_consumers=num_consumers,
        total_timesteps=total_timesteps
    )
    
    metrics, execution_time = production_line.run()
    
    return {
        'buffer_capacity': buffer_capacity,
        'num_producers': num_producers,
        'num_consumers': num_consumers,
        'total_timesteps': total_timesteps,
        'execution_time': execution_time,
        'total_produced': metrics.total_produced,
        'total_consumed': metrics.total_consumed,
        'buffer_remaining': production_line.buffer.qsize(),
        'avg_producer_wait': sum(metrics.producer_wait_times) / len(metrics.producer_wait_times) if metrics.producer_wait_times else 0,
        'avg_consumer_wait': sum(metrics.consumer_wait_times) / len(metrics.consumer_wait_times) if metrics.consumer_wait_times else 0,
        'efficiency': (metrics.total_consumed / metrics.total_produced * 100) if metrics.total_produced > 0 else 0
    }

def run_multiple_experiments():
    """Executa múltiplos experimentos com diferentes configurações"""
    experiments = [
        # Experimento 1: Configuração mínima
        {'buffer_capacity': 1000, 'num_producers': 200, 'num_consumers': 210, 'total_timesteps': 1000000},
        
        # Experimento 2: Buffer maior
        {'buffer_capacity': 2000, 'num_producers': 200, 'num_consumers': 210, 'total_timesteps': 1000000},
        
        # Experimento 3: Mais produtores
        {'buffer_capacity': 1000, 'num_producers': 300, 'num_consumers': 310, 'total_timesteps': 1000000},
        
        # Experimento 4: Desbalanceamento
        {'buffer_capacity': 1000, 'num_producers': 200, 'num_consumers': 250, 'total_timesteps': 1000000},
        
        # Experimento 5: Buffer muito grande
        {'buffer_capacity': 5000, 'num_producers': 200, 'num_consumers': 210, 'total_timesteps': 1000000},
        
        # Experimento 6: Muitos produtores e consumidores
        {'buffer_capacity': 1000, 'num_producers': 500, 'num_consumers': 510, 'total_timesteps': 1000000},
    ]
    
    results = []
    for i, exp in enumerate(experiments, 1):
        print(f"\n{'#'*60}")
        print(f"EXPERIMENTO {i} de {len(experiments)}")
        print(f"{'#'*60}")
        result = run_experiment(**exp)
        results.append(result)
    
    # Salva resultados
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f'results/production_results_{timestamp}.csv'
    df.to_csv(csv_filename, index=False)
    print(f"\n✅ Resultados salvos em {csv_filename}")
    
    # Exibe tabela comparativa
    print("\n" + "="*120)
    print("TABELA COMPARATIVA DE EXPERIMENTOS")
    print("="*120)
    print(df.to_string(index=False))
    
    # Gera gráficos
    plot_results(df, timestamp)
    
    return df

def plot_results(df, timestamp):
    """Gera gráficos comparativos dos experimentos"""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Análise Comparativa dos Experimentos de Produção', fontsize=16, fontweight='bold')
    
    # Gráfico 1: Produção vs Consumo por Experimento
    x = range(len(df))
    width = 0.35
    axes[0, 0].bar([i - width/2 for i in x], df['total_produced'], width, label='Produzidos', alpha=0.8)
    axes[0, 0].bar([i + width/2 for i in x], df['total_consumed'], width, label='Consumidos', alpha=0.8)
    axes[0, 0].set_xlabel('Experimento')
    axes[0, 0].set_ylabel('Quantidade')
    axes[0, 0].set_title('Produção vs Consumo')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Gráfico 2: Eficiência por Experimento
    axes[0, 1].plot(x, df['efficiency'], marker='o', linewidth=2, markersize=8, color='green')
    axes[0, 1].set_xlabel('Experimento')
    axes[0, 1].set_ylabel('Eficiência (%)')
    axes[0, 1].set_title('Eficiência do Sistema')
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].set_ylim([95, 101])
    
    # Gráfico 3: Tempos de Espera Médios
    axes[0, 2].bar(x, df['avg_producer_wait'] * 1000, alpha=0.7, label='Produtores')
    axes[0, 2].bar(x, df['avg_consumer_wait'] * 1000, alpha=0.7, label='Consumidores')
    axes[0, 2].set_xlabel('Experimento')
    axes[0, 2].set_ylabel('Tempo Médio (ms)')
    axes[0, 2].set_title('Tempo Médio de Espera')
    axes[0, 2].legend()
    axes[0, 2].grid(True, alpha=0.3)
    
    # Gráfico 4: Itens Restantes no Buffer
    axes[1, 0].bar(x, df['buffer_remaining'], color='orange', alpha=0.7)
    axes[1, 0].set_xlabel('Experimento')
    axes[1, 0].set_ylabel('Itens')
    axes[1, 0].set_title('Itens Restantes no Buffer')
    axes[1, 0].grid(True, alpha=0.3)
    
    # Gráfico 5: Tempo de Execução
    axes[1, 1].bar(x, df['execution_time'], color='purple', alpha=0.7)
    axes[1, 1].set_xlabel('Experimento')
    axes[1, 1].set_ylabel('Tempo (segundos)')
    axes[1, 1].set_title('Tempo de Execução')
    axes[1, 1].grid(True, alpha=0.3)
    
    # Gráfico 6: Configuração
    axes[1, 2].scatter(df['buffer_capacity'], df['num_producers'], 
                       s=100, alpha=0.6, label='Produtores', marker='o')
    axes[1, 2].scatter(df['buffer_capacity'], df['num_consumers'], 
                       s=100, alpha=0.6, label='Consumidores', marker='s')
    axes[1, 2].set_xlabel('Capacidade do Buffer')
    axes[1, 2].set_ylabel('Número de Threads')
    axes[1, 2].set_title('Configuração: Buffer vs Threads')
    axes[1, 2].legend()
    axes[1, 2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    plot_filename = f'charts/production_analysis_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"📊 Gráficos salvos em '{plot_filename}'")
    
    # Tenta exibir
    try:
        plt.show()
    except:
        print("   (Gráficos não podem ser exibidos neste ambiente, mas foram salvos)")

def generate_detailed_report(df, timestamp):
    """Gera relatório detalhado em texto"""
    report_filename = f'reports/production_report_{timestamp}.txt'
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("RELATÓRIO DETALHADO - ANÁLISE DE LINHA DE PRODUÇÃO INDUSTRIAL\n")
        f.write("="*80 + "\n\n")
        f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"Total de Experimentos: {len(df)}\n\n")
        
        f.write("-"*80 + "\n")
        f.write("RESUMO ESTATÍSTICO\n")
        f.write("-"*80 + "\n\n")
        
        f.write(f"Eficiência Média: {df['efficiency'].mean():.2f}%\n")
        f.write(f"Eficiência Mínima: {df['efficiency'].min():.2f}%\n")
        f.write(f"Eficiência Máxima: {df['efficiency'].max():.2f}%\n\n")
        
        f.write(f"Tempo Médio de Execução: {df['execution_time'].mean():.2f}s\n")
        f.write(f"Total de Itens Produzidos (soma): {df['total_produced'].sum():,}\n")
        f.write(f"Total de Itens Consumidos (soma): {df['total_consumed'].sum():,}\n\n")
        
        f.write("-"*80 + "\n")
        f.write("DETALHAMENTO POR EXPERIMENTO\n")
        f.write("-"*80 + "\n\n")
        
        for i, row in df.iterrows():
            f.write(f"EXPERIMENTO {i+1}:\n")
            f.write(f"  Configuração:\n")
            f.write(f"    - Buffer: {row['buffer_capacity']}\n")
            f.write(f"    - Produtores: {row['num_producers']}\n")
            f.write(f"    - Consumidores: {row['num_consumers']}\n")
            f.write(f"    - Timesteps: {row['total_timesteps']:,}\n")
            f.write(f"  Resultados:\n")
            f.write(f"    - Produzidos: {row['total_produced']:,}\n")
            f.write(f"    - Consumidos: {row['total_consumed']:,}\n")
            f.write(f"    - Restantes: {row['buffer_remaining']}\n")
            f.write(f"    - Eficiência: {row['efficiency']:.2f}%\n")
            f.write(f"    - Tempo: {row['execution_time']:.2f}s\n")
            f.write(f"    - Espera Prod: {row['avg_producer_wait']*1000:.4f}ms\n")
            f.write(f"    - Espera Cons: {row['avg_consumer_wait']*1000:.4f}ms\n")
            f.write("\n")
        
        f.write("="*80 + "\n")
        f.write("FIM DO RELATÓRIO\n")
        f.write("="*80 + "\n")
    
    print(f"📄 Relatório detalhado salvo em '{report_filename}'")

# Exemplo de uso
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SISTEMA DE CONTROLE DE LINHA DE PRODUÇÃO INDUSTRIAL")
    print("="*60)
    print("\nEscolha uma opção:")
    print("1. Toy Problem (teste rápido - ~2 segundos)")
    print("2. Experimentos Completos (pode demorar 15-30 minutos)")
    print("3. Experimento Personalizado")
    print("="*60)
    
    opcao = input("\nDigite o número da opção (1, 2 ou 3): ").strip()
    
    if opcao == "1":
        print("\n🧪 Executando TOY PROBLEM para teste rápido...")
        print("   (Parâmetros reduzidos para demonstração)\n")
        
        production_line = ProductionLine.__new__(ProductionLine)
        production_line.buffer_capacity = 10
        production_line.num_producers = 2
        production_line.num_consumers = 3
        production_line.total_timesteps = 100
        production_line.buffer = queue.Queue(maxsize=10)
        production_line.empty_slots = threading.Semaphore(10)
        production_line.filled_slots = threading.Semaphore(0)
        production_line.mutex = threading.Lock()
        production_line.current_timestep = 0
        production_line.timestep_lock = threading.Lock()
        production_line.running = True
        production_line.metrics = ProductionMetrics()
        production_line.metrics_lock = threading.Lock()
        production_line.producer_threads = []
        production_line.consumer_threads = []
        production_line.start_time = None
        
        production_line.run()
        
    elif opcao == "2":
        print("\n🚀 Iniciando Experimentos Completos...")
        print("⚠️  ATENÇÃO: Isso pode demorar 15-30 minutos!")
        confirma = input("Deseja continuar? (s/n): ").strip().lower()
        
        if confirma == 's':
            df_results = run_multiple_experiments()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            generate_detailed_report(df_results, timestamp)
            print("\n✅ Todos os experimentos concluídos!")
            print(f"   Arquivos gerados:")
            print(f"   - production_results_{timestamp}.csv")
            print(f"   - production_analysis_{timestamp}.png")
            print(f"   - production_report_{timestamp}.txt")
        else:
            print("Operação cancelada.")
            
    elif opcao == "3":
        print("\n⚙️  Configuração Personalizada")
        print("="*60)
        
        try:
            buffer = int(input("Capacidade do Buffer (mín. 1000): "))
            prod = int(input("Número de Produtores (mín. 200): "))
            cons = int(input("Número de Consumidores (mín. produtores + 10): "))
            steps = int(input("Timesteps (mín. 1000000): "))
            
            result = run_experiment(buffer, prod, cons, steps)
            
            print("\n✅ Experimento personalizado concluído!")
            print(f"   Eficiência: {result['efficiency']:.2f}%")
            print(f"   Tempo: {result['execution_time']:.2f}s")
            
        except ValueError:
            print("❌ Entrada inválida. Use apenas números inteiros.")
        except AssertionError as e:
            print(f"❌ Validação falhou: {e}")
    else:
        print("❌ Opção inválida. Execute o programa novamente.")